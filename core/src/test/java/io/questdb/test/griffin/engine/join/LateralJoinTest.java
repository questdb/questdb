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

package io.questdb.test.griffin.engine.join;

import io.questdb.test.AbstractCairoTest;
import org.junit.Ignore;
import org.junit.Test;

public class LateralJoinTest extends AbstractCairoTest {

    @Test
    public void testCrossJoinLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            2\t20.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            CROSS JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testEliminateOuterRefIntermediateWhere() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, b INT, threshold INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 100, 5, '2024-01-01T00:00:00.000000Z'),
                    (2, 200, 25, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 3, '2024-01-01T00:10:00.000000Z'),
                    (1, 10, '2024-01-01T00:20:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z'),
                    (2, 30, '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            a\ttotal
                            1\t13
                            2\t50
                            """,
                    """
                            SELECT t1.a, x.total
                            FROM t1
                            JOIN LATERAL (
                                SELECT sum(val) AS total
                                FROM (SELECT val, t1_id FROM t2 WHERE t2.t1_id = t1.a)
                                WHERE t1_id = t1.a
                                GROUP BY t1.a
                            ) x
                            ORDER BY t1.a
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testEliminateOuterRefLiftExpressionWithOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, b INT, threshold INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 100, 5, '2024-01-01T00:00:00.000000Z'),
                    (2, 200, 25, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 3, '2024-01-01T00:10:00.000000Z'),
                    (1, 10, '2024-01-01T00:20:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z'),
                    (2, 30, '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            a\tresult
                            1\t110
                            2\t230
                            """,
                    """
                            SELECT t1.a, x.result
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT val + t1.b AS result
                                FROM (
                                    SELECT val FROM t2 WHERE t2.t1_id = t1.a
                                )
                                WHERE val > t1.threshold
                            ) x
                            ORDER BY t1.a, x.result
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testEliminateOuterRefLiftPureOuterColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, b INT, threshold INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 100, 5, '2024-01-01T00:00:00.000000Z'),
                    (2, 200, 25, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 3, '2024-01-01T00:10:00.000000Z'),
                    (1, 10, '2024-01-01T00:20:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z'),
                    (2, 30, '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            a\tb
                            1\t100
                            1\t100
                            2\t200
                            """,
                    """
                            SELECT t1.a, x.b
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT t1.b AS b
                                FROM (
                                    SELECT val FROM t2 WHERE t2.t1_id = t1.a
                                )
                                WHERE val > t1.threshold OR val < 10
                            ) x
                            ORDER BY t1.a
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testEliminateOuterRefNoExtraColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, b INT, threshold INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 100, 5, '2024-01-01T00:00:00.000000Z'),
                    (2, 200, 25, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 3, '2024-01-01T00:10:00.000000Z'),
                    (1, 10, '2024-01-01T00:20:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z'),
                    (2, 30, '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            a\tresult
                            1\t110
                            2\t230
                            """,
                    """
                            SELECT t1.a, x.result
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT val + t1.b AS result, val + t1.b AS result1
                                FROM (
                                    SELECT val FROM t2 WHERE t2.t1_id = t1.a
                                )
                                WHERE val > t1.threshold
                            ) x
                            ORDER BY t1.a
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testEliminateOuterRefOrCondition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, b INT, threshold INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 100, 5, '2024-01-01T00:00:00.000000Z'),
                    (2, 200, 25, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 3, '2024-01-01T00:10:00.000000Z'),
                    (1, 10, '2024-01-01T00:20:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z'),
                    (2, 30, '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            a\tresult
                            1\t103
                            1\t110
                            2\t230
                            """,
                    """
                            SELECT t1.a, x.result
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT val + t1.b AS result
                                FROM (
                                    SELECT val FROM t2 WHERE t2.t1_id = t1.a
                                )
                                WHERE val > t1.threshold OR val < 5
                            ) x
                            ORDER BY t1.a, x.result
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testLateralRequiresSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            assertException(
                    "SELECT * FROM orders o JOIN LATERAL trades t",
                    36,
                    "LATERAL requires a subquery"
            );
        });
    }

    // Test LATERAL keyword as standalone (LATERAL (subquery) alias — no JOIN keyword)
    @Test
    public void testLateralStandalone() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            2\t20.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // Test LATERAL with unsupported join type (RIGHT JOIN LATERAL)
    @Test
    public void testLateralUnsupportedJoinType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO trades VALUES (1, 1, '2024-01-01T00:30:00.000000Z')");

            assertException(
                    """
                            SELECT * FROM orders o
                            RIGHT JOIN LATERAL (SELECT * FROM trades WHERE order_id = o.id) t
                            """,
                    34,
                    "LATERAL is only supported with INNER, LEFT, or CROSS joins"
            );
        });
    }

    @Test
    public void testLateralWithCorrelatedJoinModelSubqueries() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_outer (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t_a (oid INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t_b (oid INT, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t_outer VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t_a VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 30, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t_b VALUES
                    (1, 100, '2024-01-01T00:10:00.000000Z'),
                    (2, 200, '2024-01-01T01:10:00.000000Z')
                    """);

            // Both sides of the JOIN inside the lateral subquery have correlated references
            assertQueryNoLeakCheck(
                    """
                            id\tval\tqty
                            1\t10\t100
                            1\t20\t100
                            2\t30\t200
                            """,
                    """
                            SELECT o.id, sub.val, sub.qty
                            FROM t_outer o
                            JOIN LATERAL (
                                SELECT a.val, b.qty
                                FROM (SELECT val FROM t_a WHERE t_a.oid = o.id) a
                                CROSS JOIN (SELECT qty FROM t_b WHERE t_b.oid = o.id) b
                            ) sub ON true
                            ORDER BY o.id, sub.val
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testLateralWithCorrelatedLeftJoinCriteria() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_outer (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t_left (id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t_right (id INT, ref_id INT, info STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t_outer VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t_left VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z'),
                    (3, 30, '2024-01-01T02:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t_right VALUES
                    (1, 1, 'info_a', '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 'info_b', '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tval\tinfo
                            1\t10\tinfo_a
                            2\t20\tinfo_b
                            """,
                    """
                            SELECT o.id, sub.val, sub.info
                            FROM t_outer o
                            JOIN LATERAL (
                                SELECT l.val, r.info
                                FROM t_left l
                                LEFT JOIN t_right r ON l.id = r.ref_id AND r.id = o.id
                                WHERE l.id = o.id
                            ) sub ON true
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testLateralWithUnionAllBuckets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z'),
                    (4, '2024-01-01T03:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 25.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 40.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 15.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 50.0, '2024-01-01T01:20:00.000000Z'),
                    (6, 3, 29.0, '2024-01-01T02:10:00.000000Z'),
                    (7, 3, 30.0, '2024-01-01T02:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tqty\tbucket
                            1\t10.0\tsmall
                            1\t25.0\tsmall
                            1\t40.0\tlarge
                            2\t15.0\tsmall
                            2\t50.0\tlarge
                            3\t29.0\tsmall
                            3\t30.0\tlarge
                            """,
                    """
                            SELECT o.id, t.qty, t.bucket
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty, 'small' AS bucket FROM trades
                                    WHERE order_id = o.id AND qty < 30
                                UNION ALL
                                SELECT qty, 'large' AS bucket FROM trades
                                    WHERE order_id = o.id AND qty >= 30
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testLateralWithUnionAndLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_outer (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t_data (oid INT, val INT, src STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t_outer VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t_data VALUES
                    (1, 10, 'A', '2024-01-01T00:10:00.000000Z'),
                    (1, 20, 'A', '2024-01-01T00:20:00.000000Z'),
                    (1, 30, 'A', '2024-01-01T00:30:00.000000Z'),
                    (1, 40, 'B', '2024-01-01T00:40:00.000000Z'),
                    (1, 50, 'B', '2024-01-01T00:50:00.000000Z'),
                    (2, 60, 'A', '2024-01-01T01:10:00.000000Z'),
                    (2, 70, 'B', '2024-01-01T01:20:00.000000Z'),
                    (2, 80, 'B', '2024-01-01T01:30:00.000000Z')
                    """);

            // UNION branches with LIMIT
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\t10
                            1\t40
                            2\t60
                            2\t70
                            """,
                    """
                            SELECT o.id, sub.val
                            FROM t_outer o
                            JOIN LATERAL (
                                (SELECT val FROM t_data WHERE oid = o.id AND src = 'A' ORDER BY val LIMIT 1)
                                UNION ALL
                                (SELECT val FROM t_data WHERE oid = o.id AND src = 'B' ORDER BY val LIMIT 1)
                            ) sub ON true
                            ORDER BY o.id, sub.val
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testLateralWithUnionDeepAggregate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (x INT, a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t3 (x INT, b INT, cat STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t3 VALUES
                    (1, 100, 'A', '2024-01-01T00:10:00.000000Z'),
                    (1, 200, 'B', '2024-01-01T00:20:00.000000Z'),
                    (2, 300, 'A', '2024-01-01T01:10:00.000000Z'),
                    (2, 400, 'A', '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            x\tval
                            1\t10
                            1\t200
                            2\t20
                            2\t700
                            """,
                    """
                            SELECT t1.x, sub.val FROM t1
                            CROSS JOIN LATERAL (
                                SELECT a AS val FROM t2 WHERE t2.x = t1.x
                                UNION ALL
                                SELECT total FROM (
                                    SELECT sum(b) AS total FROM t3 WHERE t3.x = t1.x GROUP BY cat
                                ) agg WHERE agg.total > 100
                            ) sub
                            ORDER BY t1.x, sub.val
                            """,
                    null, true, true
            );
        });
    }

    // UNION branch with deeper nesting: correlated ref inside a subquery within a UNION branch
    @Test
    public void testLateralWithUnionDeepNesting() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (x INT, a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t3 (x INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 30, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t3 VALUES
                    (1, 100, '2024-01-01T00:10:00.000000Z'),
                    (2, 200, '2024-01-01T01:10:00.000000Z'),
                    (2, 300, '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            x\tval
                            1\t30
                            1\t100
                            2\t30
                            2\t500
                            """,
                    """
                            SELECT t1.x, sub.val FROM t1
                            CROSS JOIN LATERAL (
                                SELECT sum(a) AS val FROM t2 WHERE t2.x = t1.x
                                UNION ALL
                                SELECT s FROM (
                                    SELECT sum(b) AS s FROM t3 WHERE t3.x = t1.x
                                ) nested_sub
                            ) sub
                            ORDER BY t1.x, sub.val
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testLateralWithUnionOnNonJoinLayer() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_outer (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t_data (oid INT, val INT, src STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t_outer VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t_data VALUES
                    (1, 10, 'A', '2024-01-01T00:10:00.000000Z'),
                    (1, 20, 'B', '2024-01-01T00:20:00.000000Z'),
                    (2, 30, 'A', '2024-01-01T01:10:00.000000Z'),
                    (2, 40, 'B', '2024-01-01T01:20:00.000000Z')
                    """);

            // UNION on a non-join layer (wrapped in a SELECT)
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\t10
                            1\t20
                            2\t30
                            2\t40
                            """,
                    """
                            SELECT o.id, sub.val
                            FROM t_outer o
                            JOIN LATERAL (
                                SELECT val FROM (
                                    SELECT val FROM t_data WHERE oid = o.id AND src = 'A'
                                    UNION ALL
                                    SELECT val FROM t_data WHERE oid = o.id AND src = 'B'
                                )
                            ) sub ON true
                            ORDER BY o.id, sub.val
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testLeftLateralCountMixedPrefixColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE fx_trades (timestamp TIMESTAMP, symbol SYMBOL, price DOUBLE) TIMESTAMP(timestamp) PARTITION BY DAY");
            execute("""
                    INSERT INTO fx_trades VALUES
                    ('2024-01-01T00:00:00.000000Z', 'EUR/USD', 1.10),
                    ('2024-01-01T01:00:00.000000Z', 'EUR/USD', 1.20),
                    ('2024-01-01T02:00:00.000000Z', 'GBP/USD', 1.30)
                    """);

            assertQueryNoLeakCheck(
                    """
                            timestamp\tsymbol\tprice\tc
                            2024-01-01T00:00:00.000000Z\tEUR/USD\t1.1\t1
                            2024-01-01T01:00:00.000000Z\tEUR/USD\t1.2\t0
                            2024-01-01T02:00:00.000000Z\tGBP/USD\t1.3\t0
                            """,
                    """
                            SELECT timestamp, t.symbol, price, c FROM fx_trades t
                            LEFT JOIN LATERAL (
                                SELECT count() c FROM fx_trades
                                WHERE symbol = t.symbol AND price > (t.price * 1.01)
                            ) u
                            ORDER BY timestamp
                            """,
                    "timestamp", false, false
            );

            assertQueryNoLeakCheck(
                    """
                            timestamp\tsymbol\tprice\tc
                            2024-01-01T00:00:00.000000Z\tEUR/USD\t1.1\t1
                            2024-01-01T01:00:00.000000Z\tEUR/USD\t1.2\t0
                            2024-01-01T02:00:00.000000Z\tGBP/USD\t1.3\t0
                            """,
                    """
                            SELECT timestamp, t.symbol, price, c FROM fx_trades t
                            LEFT JOIN LATERAL (
                                SELECT count() c FROM fx_trades
                                WHERE symbol = t.symbol AND price > (t.price * 1.01)
                            )
                            ORDER BY timestamp
                            """,
                    "timestamp", false, false
            );

            assertQueryNoLeakCheck(
                    """
                            timestamp	symbol	price	symbol1	c
                            2024-01-01T00:00:00.000000Z	EUR/USD	1.1	EUR/USD	1
                            2024-01-01T01:00:00.000000Z	EUR/USD	1.2		0
                            2024-01-01T02:00:00.000000Z	GBP/USD	1.3		0
                            """,
                    """
                            SELECT timestamp, t.symbol, price, u.symbol, c FROM fx_trades t
                            LEFT JOIN LATERAL (
                                SELECT symbol, count() c FROM fx_trades
                                WHERE symbol = t.symbol AND price > (t.price * 1.01)
                            ) u
                            ORDER BY timestamp
                            """,
                    "timestamp", false, false
            );
        });
    }

    @Test
    public void testPerSidePushInnerBranch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE base_data (order_id INT, category STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO base_data VALUES
                    (1, 'A', '2024-01-01T00:05:00.000000Z'),
                    (2, 'B', '2024-01-01T01:05:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tcategory\ttrade_total
                            1\tA\t30.0
                            2\tB\t30.0
                            """,
                    """
                            SELECT o.id, sub.category, sub.trade_total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT b.category, t.trade_total
                                FROM base_data b
                                INNER JOIN (
                                    SELECT sum(qty) AS trade_total, order_id
                                    FROM trades
                                    WHERE order_id = o.id
                                    GROUP BY order_id
                                ) t ON b.order_id = t.order_id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testPerSidePushIntermediateLayerProjection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE base_data (order_id INT, category STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO base_data VALUES
                    (1, 'A', '2024-01-01T00:05:00.000000Z'),
                    (2, 'B', '2024-01-01T01:05:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\ttrade_total
                            1\t30.0
                            2\t30.0
                            """,
                    """
                            SELECT o.id, sub.trade_total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT s2.trade_total
                                FROM (
                                    SELECT s1.trade_total
                                    FROM (
                                        SELECT t.trade_total
                                        FROM base_data b
                                        INNER JOIN (
                                            SELECT sum(qty) AS trade_total, order_id
                                            FROM trades
                                            WHERE order_id = o.id
                                            GROUP BY order_id
                                        ) t ON b.order_id = t.order_id
                                    ) s1
                                ) s2
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testPerSidePushLeftBranchFallback() throws Exception {
        // LEFT branch inside the lateral subquery should NOT use per-side push
        // because LEFT preserves unmatched rows. This test verifies the normal
        // decorrelation still works for LEFT JOIN LATERAL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\ttotal
                            1\t30
                            2\t0
                            3\t0
                            """,
                    """
                            SELECT t1.id, coalesce(sub.total, 0) AS total
                            FROM t1
                            LEFT JOIN LATERAL (
                                SELECT sum(val) AS total
                                FROM t2
                                WHERE t2.t1_id = t1.id
                            ) sub
                            ORDER BY t1.id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testPerSidePushMultipleBranches() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE base_data (order_id INT, category STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE returns (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO base_data VALUES
                    (1, 'A', '2024-01-01T00:05:00.000000Z'),
                    (2, 'B', '2024-01-01T01:05:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO returns VALUES
                    (1, 5.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 15.0, '2024-01-01T01:30:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tcategory\ttrade_total\treturn_total
                            1\tA\t30.0\t5.0
                            2\tB\t30.0\t15.0
                            """,
                    """
                            SELECT o.id, sub.category, sub.trade_total, sub.return_total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT b.category, t.trade_total, r.return_total
                                FROM base_data b
                                INNER JOIN (
                                    SELECT sum(qty) AS trade_total, order_id
                                    FROM trades
                                    WHERE order_id = o.id
                                    GROUP BY order_id
                                ) t ON b.order_id = t.order_id
                                INNER JOIN (
                                    SELECT sum(qty) AS return_total, order_id
                                    FROM returns
                                    WHERE order_id = o.id
                                    GROUP BY order_id
                                ) r ON b.order_id = r.order_id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testPerSidePushSimpleLateral() throws Exception {
        // Simple lateral join (no multi-table join inside the lateral subquery).
        // Per-side push is NOT applicable here since the main chain has correlation.
        // This test ensures normal decorrelation still works correctly.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 30, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\ttotal
                            1\t30
                            2\t30
                            """,
                    """
                            SELECT t1.id, sub.total
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT sum(val) AS total
                                FROM t2
                                WHERE t2.t1_id = t1.id
                            ) sub
                            ORDER BY t1.id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT01ScanInner() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQueryNoLeakCheck(
                    """
                            id\tcustomer\tqty
                            1\tAlice\t10.0
                            1\tAlice\t20.0
                            2\tBob\t30.0
                            3\tCharlie\t40.0
                            3\tCharlie\t50.0
                            """,
                    """
                            SELECT o.id, o.customer, t.qty
                            FROM orders o
                            JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T02: Scan + WHERE, LEFT, equality correlation — NULL fill for unmatched rows
    @Test
    public void testT02ScanLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, customer STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'Alice', '2024-01-01T00:00:00.000000Z'),
                    (2, 'Bob', '2024-01-01T01:00:00.000000Z'),
                    (3, 'Charlie', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:45:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tcustomer\tqty
                            1\tAlice\t10.0
                            1\tAlice\t20.0
                            2\tBob\tnull
                            3\tCharlie\tnull
                            """,
                    """
                            SELECT o.id, o.customer, t.qty
                            FROM orders o
                            LEFT JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T03: GROUP BY + SUM, INNER, equality correlation — GROUP BY expansion
    @Test
    public void testT03GroupByInner() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQueryNoLeakCheck(
                    """
                            id\tcustomer\ttotal_qty
                            1\tAlice\t30.0
                            2\tBob\t30.0
                            3\tCharlie\t90.0
                            """,
                    """
                            SELECT o.id, o.customer, t.total_qty
                            FROM orders o
                            JOIN LATERAL (SELECT sum(qty) AS total_qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT04GroupByCountLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tcnt
                            1\t2
                            2\t1
                            3\t0
                            """,
                    """
                            SELECT o.id, t.cnt
                            FROM orders o
                            LEFT JOIN LATERAL (SELECT count(*) AS cnt FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T05: Window(sum OVER), INNER, equality correlation — PARTITION BY expansion
    @Test
    public void testT05WindowInner() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQueryNoLeakCheck(
                    """
                            id\tqty\trunning_total
                            1\t10.0\t10.0
                            1\t20.0\t30.0
                            3\t40.0\t40.0
                            3\t50.0\t90.0
                            """,
                    """
                            SELECT o.id, t.qty, t.running_total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty, sum(qty) OVER (ORDER BY ts) AS running_total
                                FROM trades
                                WHERE order_id = o.id
                            ) t
                            WHERE o.id IN (1, 3)
                            ORDER BY o.id, t.qty
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testT06WindowWithFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE mm (mm_id INT, symbol STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE fills (id INT, mm_id INT, symbol STRING, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO mm VALUES
                    (1, 'AAPL', '2024-01-01T00:00:00.000000Z'),
                    (2, 'GOOG', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO fills VALUES
                    (1, 1, 'AAPL', 100.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'AAPL', 200.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 'AAPL', 150.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 'GOOG', 300.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 'GOOG', 400.0, '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            mm_id\tsymbol\tqty\trunning_sum
                            1\tAAPL\t150.0\t350.0
                            1\tAAPL\t200.0\t200.0
                            2\tGOOG\t300.0\t300.0
                            2\tGOOG\t400.0\t700.0
                            """,
                    """
                            SELECT m.mm_id, m.symbol, f.qty, f.running_sum
                            FROM mm m
                            JOIN LATERAL (
                                SELECT qty, sum(qty) OVER (ORDER BY ts) AS running_sum
                                FROM fills
                                WHERE mm_id = m.mm_id AND symbol = m.symbol AND qty >= 150.0
                            ) f
                            ORDER BY m.mm_id, f.qty
                            """,
                    null, true, true
            );
        });
    }

    // T07: ORDER BY + LIMIT, INNER, equality correlation — row_number rewrite
    @Test
    public void testT07LimitInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, customer STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'Alice', '2024-01-01T00:00:00.000000Z'),
                    (2, 'Bob', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 50.0, '2024-01-01T01:20:00.000000Z'),
                    (6, 2, 60.0, '2024-01-01T01:30:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tcustomer\tqty
                            1\tAlice\t30.0
                            1\tAlice\t20.0
                            2\tBob\t60.0
                            2\tBob\t50.0
                            """,
                    """
                            SELECT o.id, o.customer, t.qty
                            FROM orders o
                            JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id ORDER BY qty DESC LIMIT 2) t
                            ORDER BY o.id, t.qty DESC
                            """,
                    null, true, false
            );
        });
    }

    // T08: DISTINCT, INNER, equality correlation
    @Test
    public void testT08DistinctInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, category STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'A', '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'A', '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 'B', '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 'C', '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 'C', '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tcategory
                            1\tA
                            1\tB
                            2\tC
                            """,
                    """
                            SELECT o.id, t.category
                            FROM orders o
                            JOIN LATERAL (SELECT DISTINCT category FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id, t.category
                            """,
                    null, true, false
            );
        });
    }

    // T09: UNION ALL, INNER, equality correlation
    @Test
    public void testT09UnionAll() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("INSERT INTO trades_a VALUES (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'), (2, 2, 20.0, '2024-01-01T01:10:00.000000Z')");
            execute("INSERT INTO trades_b VALUES (1, 1, 30.0, '2024-01-01T00:20:00.000000Z'), (2, 2, 40.0, '2024-01-01T01:20:00.000000Z')");

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            1\t30.0
                            2\t20.0
                            2\t40.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                UNION ALL
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, true
            );
        });
    }

    // T100: LEFT LATERAL count referenced in expressions — coalesce must wrap nested refs too
    @Test
    public void testT100LeftCountInExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, '2024-01-01T00:10:00.000000Z'),
                    (1, '2024-01-01T00:20:00.000000Z'),
                    (2, '2024-01-01T01:10:00.000000Z')
                    """);

            // order 3 has no trades: cnt should be 0, cnt + 1 should be 1 (not NULL)
            assertQueryNoLeakCheck(
                    """
                            id\tcnt\tcnt_plus_one
                            1\t2\t3
                            2\t1\t2
                            3\t0\t1
                            """,
                    """
                            SELECT o.id, sub.cnt, sub.cnt + 1 AS cnt_plus_one
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT count(*) AS cnt FROM trades WHERE order_id = o.id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T100: RIGHT JOIN branch inside lateral subquery with correlated ON
    @Test
    public void testT100RightJoinBranchInsideLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE adjustments (order_id INT, adj DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z')
                    """);
            // Adjustments exist for both orders, but trades only for order 1
            execute("""
                    INSERT INTO adjustments VALUES
                    (1, 1.5, '2024-01-01T00:00:00.000000Z'),
                    (2, 2.0, '2024-01-01T01:00:00.000000Z')
                    """);

            // RIGHT JOIN: adjustments is the preserved side
            // For order 1: adj(1,1.5) matches trades(1,10),(1,20) → (10,1.5),(20,1.5)
            // For order 2: adj(2,2.0) has no matching trades → (null,2.0)
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tadj
                            1\t10.0\t1.5
                            1\t20.0\t1.5
                            2\tnull\t2.0
                            """,
                    """
                            SELECT o.id, sub.qty, sub.adj
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, a.adj
                                FROM trades t
                                RIGHT JOIN adjustments a ON a.order_id = t.order_id
                                WHERE a.order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.qty
                            """,
                    null, true, true
            );
        });
    }

    // T100b: FULL OUTER JOIN branch inside lateral subquery
    @Test
    public void testT100bFullOuterJoinBranchInsideLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE refunds (order_id INT, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z')
                    """);
            // Refund only for order 2
            execute("""
                    INSERT INTO refunds VALUES
                    (2, 5.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // FULL OUTER JOIN: both sides preserved
            // For order 1: trades match, no refunds → (10, null), (20, null)
            // For order 2: no trades, refund matches → (null, 5.0)
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tamount
                            1\t10.0\tnull
                            1\t20.0\tnull
                            2\tnull\t5.0
                            """,
                    """
                            SELECT o.id, sub.qty, sub.amount
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, r.amount
                                FROM trades t
                                FULL OUTER JOIN refunds r ON r.order_id = t.order_id
                                WHERE t.order_id = o.id OR r.order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.qty
                            """,
                    null, true, true
            );
        });
    }

    // T100c: mixed LEFT + RIGHT join branches inside lateral subquery
    @Test
    public void testT100cMixedLeftRightJoinBranchesInsideLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE adjustments (order_id INT, adj DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE discounts (order_id INT, disc DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            // Adjustments only for order 1
            execute("""
                    INSERT INTO adjustments VALUES
                    (1, 1.5, '2024-01-01T00:00:00.000000Z')
                    """);
            // Discounts only for order 2
            execute("""
                    INSERT INTO discounts VALUES
                    (2, 0.8, '2024-01-01T01:00:00.000000Z')
                    """);

            // LEFT JOIN adjustments: order 2 has no adj → null
            // RIGHT JOIN discounts: order 1 has no disc → trade(1,10) is dropped
            //   by RIGHT JOIN (only right side is preserved)
            // For order 1: no matching discount → RIGHT JOIN drops all rows → empty
            // For order 2: trade(30) matches disc(0.8), no adj → (30, null, 0.8)
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tadj\tdisc
                            2\t30.0\tnull\t0.8
                            """,
                    """
                            SELECT o.id, sub.qty, sub.adj, sub.disc
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, a.adj, d.disc
                                FROM trades t
                                LEFT JOIN adjustments a ON a.order_id = t.order_id
                                RIGHT JOIN discounts d ON d.order_id = t.order_id
                                WHERE t.order_id = o.id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    // T102: WINDOW JOIN inside lateral subquery
    @Test
    public void testT102WindowJoinInsideLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE instruments (id INT, tag SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (instrument_id INT, price DOUBLE, tag SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE quotes (price DOUBLE, tag SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO instruments VALUES
                    (1, 'A', '2024-01-01T00:00:00.000000Z'),
                    (2, 'B', '2024-01-01T00:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, 'A', '2024-01-01T00:01:00.000000Z'),
                    (1, 11.0, 'A', '2024-01-01T00:02:00.000000Z'),
                    (2, 20.0, 'B', '2024-01-01T00:01:00.000000Z')
                    """);
            execute("""
                    INSERT INTO quotes VALUES
                    (9.5, 'A', '2024-01-01T00:00:30.000000Z'),
                    (10.5, 'A', '2024-01-01T00:01:30.000000Z'),
                    (19.0, 'B', '2024-01-01T00:00:30.000000Z')
                    """);

            // WINDOW JOIN inside lateral: for each instrument, compute
            // sum of (trade_price + quote_price) over a time window
            assertQueryNoLeakCheck(
                    """
                            id	sum
                            1	19.5
                            1	42.0
                            2	39.0
                            """,
                    """
                            SELECT i.id, sub.sum
                            FROM instruments i
                            JOIN LATERAL (
                                SELECT sum(t.price + q.price) AS sum
                                FROM trades t
                                WINDOW JOIN quotes q ON tag
                                    RANGE BETWEEN 1 MINUTE PRECEDING AND CURRENT ROW
                                WHERE t.instrument_id = i.id
                            ) sub
                            ORDER BY i.id, sub.sum
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testT102aHorizonJoinInsideLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE instruments (id INT, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE quotes (symbol SYMBOL, bid DOUBLE, ask DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO instruments VALUES
                    (1, 'AAPL', '2024-01-01T00:00:00.000000Z'),
                    (2, 'GOOG', '2024-01-01T00:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    ('AAPL', 100.0, '2024-01-01T00:01:00.000000Z'),
                    ('GOOG', 200.0, '2024-01-01T00:01:00.000000Z')
                    """);
            execute("""
                    INSERT INTO quotes VALUES
                    ('AAPL', 99.0, 101.0, '2024-01-01T00:00:30.000000Z'),
                    ('AAPL', 100.0, 102.0, '2024-01-01T00:01:30.000000Z'),
                    ('GOOG', 198.0, 202.0, '2024-01-01T00:00:30.000000Z'),
                    ('GOOG', 199.0, 201.0, '2024-01-01T00:02:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\thorizon_sec\tavg_mid
                            1\t0\t100.0
                            1\t1\t100.0
                            2\t0\t200.0
                            2\t1\t200.0
                            """,
                    """
                            SELECT i.id, sub.horizon_sec, sub.avg_mid
                            FROM instruments i
                            JOIN LATERAL (
                                SELECT h.offset / 1000000 AS horizon_sec,
                                       avg((q.bid + q.ask) / 2) AS avg_mid
                                FROM trades t
                                HORIZON JOIN quotes q ON (symbol)
                                    LIST (0, 1s) AS h
                                WHERE t.symbol = i.symbol
                                GROUP BY horizon_sec
                            ) sub
                            ORDER BY i.id, sub.horizon_sec
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT102bHorizonJoinRangeInsideLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE instruments (id INT, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE quotes (symbol SYMBOL, bid DOUBLE, ask DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO instruments VALUES
                    (1, 'AAPL', '2024-01-01T00:00:00.000000Z'),
                    (2, 'GOOG', '2024-01-01T00:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    ('AAPL', 100.0, 10.0, '2024-01-01T00:01:00.000000Z'),
                    ('AAPL', 101.0, 20.0, '2024-01-01T00:02:00.000000Z'),
                    ('GOOG', 200.0, 30.0, '2024-01-01T00:01:00.000000Z')
                    """);
            execute("""
                    INSERT INTO quotes VALUES
                    ('AAPL', 99.0, 101.0, '2024-01-01T00:00:30.000000Z'),
                    ('AAPL', 100.0, 102.0, '2024-01-01T00:01:30.000000Z'),
                    ('GOOG', 198.0, 202.0, '2024-01-01T00:00:30.000000Z')
                    """);

            // RANGE FROM 0s TO 0s STEP 1s = single offset at 0 (ASOF match)
            // For instrument 1: 2 trades → avg(mid) at horizon 0
            // For instrument 2: 1 trade → avg(mid) at horizon 0
            assertQueryNoLeakCheck(
                    """
                            id\tcount\tavg_mid
                            1\t2\t100.5
                            2\t1\t200.0
                            """,
                    """
                            SELECT i.id, sub.n AS count, sub.avg_mid
                            FROM instruments i
                            JOIN LATERAL (
                                SELECT count() AS n, avg((q.bid + q.ask) / 2) AS avg_mid
                                FROM trades t
                                HORIZON JOIN quotes q ON (symbol)
                                    RANGE FROM 0s TO 0s STEP 1s AS h
                                WHERE t.symbol = i.symbol
                            ) sub
                            ORDER BY i.id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT102cLeftLateralHorizonJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE instruments (id INT, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE quotes (symbol SYMBOL, bid DOUBLE, ask DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO instruments VALUES
                    (1, 'AAPL', '2024-01-01T00:00:00.000000Z'),
                    (2, 'GOOG', '2024-01-01T00:00:00.000000Z'),
                    (3, 'MSFT', '2024-01-01T00:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    ('AAPL', 100.0, '2024-01-01T00:01:00.000000Z'),
                    ('GOOG', 200.0, '2024-01-01T00:01:00.000000Z')
                    """);
            execute("""
                    INSERT INTO quotes VALUES
                    ('AAPL', 99.0, 101.0, '2024-01-01T00:00:30.000000Z'),
                    ('GOOG', 198.0, 202.0, '2024-01-01T00:00:30.000000Z')
                    """);

            // MSFT has no trades → LEFT LATERAL returns NULL for sub columns
            assertQueryNoLeakCheck(
                    """
                            id	n	avg_mid
                            1	1	100.0
                            2	1	200.0
                            3	0	null
                            """,
                    """
                            SELECT i.id, sub.n, sub.avg_mid
                            FROM instruments i
                            LEFT JOIN LATERAL (
                                SELECT count() AS n, avg((q.bid + q.ask) / 2) AS avg_mid
                                FROM trades t
                                HORIZON JOIN quotes q ON (symbol)
                                    RANGE FROM 0s TO 0s STEP 1s AS h
                                WHERE t.symbol = i.symbol
                            ) sub ON true
                            ORDER BY i.id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT103ReplaceColumnRefBinaryExpr() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tnext_id\tqty
                            1\t2\t10.0
                            2\t3\t30.0
                            """,
                    """
                            SELECT o.id, sub.order_id + 1 AS next_id, sub.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT order_id, qty FROM trades WHERE order_id = o.id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT103bReplaceColumnRefMultiArgFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, qty2 DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, null, '2024-01-01T00:10:00.000000Z'),
                    (2, null, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tresult
                            1\t10.0
                            2\t30.0
                            """,
                    """
                            SELECT o.id, coalesce(sub.qty, sub.qty2, 0) AS result
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty, qty2 FROM trades WHERE order_id = o.id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T103c: nested expressions in outer SELECT — (sub.qty * 2) + (sub.qty - 1).
    @Test
    public void testT103cReplaceColumnRefNestedExpr() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // (10*2)+(10-1)=29, (30*2)+(30-1)=89
            assertQueryNoLeakCheck(
                    """
                            id\tresult
                            1\t29.0
                            2\t89.0
                            """,
                    """
                            SELECT o.id, (sub.qty * 2) + (sub.qty - 1) AS result
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades WHERE order_id = o.id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T104: self-join lateral — lateral subquery queries the same table as outer
    @Test
    public void testT104SelfJoinLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, parent_id INT, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 0, 100.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 1, 50.0, '2024-01-01T01:00:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T02:00:00.000000Z'),
                    (4, 2, 20.0, '2024-01-01T03:00:00.000000Z')
                    """);

            // Self-join: for each order, find child orders (where parent_id = o.id)
            assertQueryNoLeakCheck(
                    """
                            id\tchild_id\tchild_amount
                            1\t2\t50.0
                            1\t3\t30.0
                            2\t4\t20.0
                            """,
                    """
                            SELECT o.id, sub.child_id, sub.child_amount
                            FROM orders o
                            JOIN LATERAL (
                                SELECT id AS child_id, amount AS child_amount
                                FROM orders
                                WHERE parent_id = o.id
                            ) sub
                            ORDER BY o.id, sub.child_id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT105UnsupportedJoinLateralError() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            assertException(
                    "SELECT * FROM t1 ASOF JOIN LATERAL (SELECT * FROM t2 WHERE x = t1.x) t",
                    27,
                    "LATERAL is only supported with INNER, LEFT, or CROSS joins"
            );

            assertException(
                    "SELECT * FROM t1 SPLICE JOIN LATERAL (SELECT * FROM t2 WHERE x = t1.x) t",
                    29,
                    "LATERAL is only supported with INNER, LEFT, or CROSS joins"
            );

            assertException(
                    "SELECT * FROM t1 FULL OUTER JOIN LATERAL (SELECT * FROM t2 WHERE x = t1.x) t ON true",
                    33,
                    "LATERAL is only supported with INNER, LEFT, or CROSS joins"
            );
        });
    }

    // T106: LEFT JOIN with correlated ON inside lateral
    @Test
    public void testT106LeftJoinCorrelatedOnSemantics() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE adjustments (order_id INT, adj DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            // Adjustment only for order 1
            execute("""
                    INSERT INTO adjustments VALUES
                    (1, 1.5, '2024-01-01T00:00:00.000000Z')
                    """);

            // LEFT JOIN ON a.order_id = o.id: correlated ON
            // Order 2's trade has no matching adjustment → adj = NULL
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tadj
                            1\t10.0\t1.5
                            2\t30.0\tnull
                            """,
                    """
                            SELECT o.id, sub.qty, sub.adj
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, a.adj
                                FROM trades t
                                LEFT JOIN adjustments a ON a.order_id = o.id AND a.order_id = t.order_id
                                WHERE t.order_id = o.id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    // T106b: RIGHT JOIN with correlated ON inside lateral.
    @Test
    public void testT106bRightJoinCorrelatedOnSemantics() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE adjustments (order_id INT, adj DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z')
                    """);
            // Adjustments for both orders
            execute("""
                    INSERT INTO adjustments VALUES
                    (1, 1.5, '2024-01-01T00:00:00.000000Z'),
                    (2, 2.0, '2024-01-01T01:00:00.000000Z')
                    """);

            // RIGHT JOIN ON a.order_id = o.id: preserves all adjustments,
            // but WHERE a.order_id = o.id filters after join.
            // o.id=1: trade(10)+adj(1.5) match; adj(2.0) preserved by RIGHT
            //   but WHERE filters it (2≠1) → only (10, 1.5)
            // o.id=2: no trades; adj(2.0) preserved, WHERE passes (2=2) → (null, 2.0)
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tadj
                            1\t10.0\t1.5
                            2\tnull\t2.0
                            """,
                    """
                            SELECT o.id, sub.qty, sub.adj
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, a.adj
                                FROM trades t
                                RIGHT JOIN adjustments a ON a.order_id = o.id AND a.order_id = t.order_id
                                WHERE a.order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.adj
                            """,
                    null, true, true
            );
        });
    }

    // T106c: FULL OUTER JOIN with correlated ON inside lateral
    @Test
    public void testT106cFullOuterJoinCorrelatedOnSemantics() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE refunds (order_id INT, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z')
                    """);
            execute("""
                    INSERT INTO refunds VALUES
                    (2, 5.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // FULL OUTER: both sides preserved
            // o.id=1: trades match, no refunds with order_id=1 → (10, null), (20, null)
            // o.id=2: no trades with order_id=2, refund(5.0) with order_id=2 → (null, 5.0)
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tamount
                            1\t10.0\tnull
                            1\t20.0\tnull
                            2\tnull\t5.0
                            """,
                    """
                            SELECT o.id, sub.qty, sub.amount
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, r.amount
                                FROM trades t
                                FULL OUTER JOIN refunds r ON r.order_id = t.order_id
                                WHERE t.order_id = o.id OR r.order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.qty
                            """,
                    null, true, true
            );
        });
    }

    // T106d: RIGHT JOIN with correlated ON — subquery branch (not table model)
    @Test
    public void testT106dRightJoinSubqueryBranchCorrelatedOn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE adjustments (order_id INT, adj DOUBLE, active INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO adjustments VALUES
                    (1, 1.5, 1, '2024-01-01T00:00:00.000000Z'),
                    (2, 2.0, 1, '2024-01-01T01:00:00.000000Z')
                    """);

            // RIGHT JOIN with subquery branch + correlated ON.
            // Correlated ON rewritten in place, not moved to WHERE
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tadj
                            1\t10.0\t1.5
                            2\tnull\t2.0
                            """,
                    """
                            SELECT o.id, sub.qty, sub.adj
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, a.adj
                                FROM trades t
                                RIGHT JOIN (SELECT order_id, adj FROM adjustments WHERE active = 1) a
                                    ON a.order_id = o.id AND a.order_id = t.order_id
                                WHERE a.order_id = o.id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    // T106e: LEFT JOIN with non-equality correlated ON inside lateral
    @Test
    public void testT106eLeftJoinNonEqCorrelatedOn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, min_qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE bonuses (trade_order_id INT, bonus DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 15.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 5.0, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO bonuses VALUES
                    (1, 100.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 200.0, '2024-01-01T01:00:00.000000Z')
                    """);

            // LEFT JOIN ON with non-eq correlated predicate: b.bonus > o.min_qty
            // o.id=1 (min_qty=15): trades(10,20), bonus(100>15)→match → (10,100),(20,100)
            // o.id=2 (min_qty=5): trades(30), bonus(200>5)→match → (30,200)
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tbonus
                            1\t10.0\t100.0
                            1\t20.0\t100.0
                            2\t30.0\t200.0
                            """,
                    """
                            SELECT o.id, sub.qty, sub.bonus
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, b.bonus
                                FROM trades t
                                LEFT JOIN bonuses b ON b.trade_order_id = t.order_id AND b.bonus > o.min_qty
                                WHERE t.order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.qty
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testT107CorrelatedJoinAboveDataSourceLevel() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE adjustments (order_id INT, adj DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO adjustments VALUES
                    (1, 1.5, '2024-01-01T00:00:00.000000Z'),
                    (2, 2.0, '2024-01-01T01:00:00.000000Z')
                    """);

            // The join with adjustments is at the SELECT level (above trades),
            // not at the data source level where terminateHere runs.
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tadj
                            1\t10.0\t1.5
                            1\t20.0\t1.5
                            2\t30.0\t2.0
                            """,
                    """
                            SELECT o.id, sub.qty, sub.adj
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, a.adj
                                FROM (SELECT order_id, qty FROM trades WHERE order_id = o.id) t
                                JOIN adjustments a ON a.order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.qty
                            """,
                    null, true, true
            );
        });
    }

    // UNION branch where only one branch is correlated, other is not.
    @Test
    public void testT108aTerminateNonCorrelatedUnionBranch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE returns (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO returns VALUES
                    (1, 5.0, '2024-01-01T00:30:00.000000Z')
                    """);

            // Both UNION branches correlated
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t5.0
                            1\t10.0
                            2\t30.0
                            """,
                    """
                            SELECT o.id, sub.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades WHERE order_id = o.id
                                UNION ALL
                                SELECT qty FROM returns WHERE order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.qty
                            """,
                    null, true, true
            );
        });
    }

    // T108b: terminate=3 — deeply correlated, nested subquery itself has correlation.
    @Test
    public void testT108bTerminateDeepCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, min_qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 15.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 5.0, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // WHERE has two correlations: order_id = o.id AND qty > o.min_qty
            // Both are at the table level → terminate=3 not needed, but tests
            // that non-eq correlation doesn't prevent correct results
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t20.0
                            2\t30.0
                            """,
                    """
                            SELECT o.id, sub.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades
                                WHERE order_id = o.id AND qty > o.min_qty
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT108cWhereCorrelatedOverNonCorrelatedSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 'A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 'B', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 'A', 30.0, '2024-01-01T01:10:00.000000Z'),
                    (2, 'A', 40.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Inner FROM is a non-correlated subquery with GROUP BY.
            // Outer SELECT WHERE filters by o.id on the subquery result.
            // The GROUP BY runs once (un-multiplied); CROSS JOIN happens above it.
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\ttotal
                            1\tA\t10.0
                            1\tB\t20.0
                            2\tA\t70.0
                            """,
                    """
                            SELECT o.id, sub.category, sub.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, total
                                FROM (SELECT order_id, category, sum(qty) AS total
                                      FROM trades GROUP BY order_id, category)
                                WHERE order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.category
                            """,
                    null, true, false
            );
        });
    }

    // T109: correlated LIMIT with GROUP BY — each outer row limits its own group count
    @Test
    public void testT109CorrelatedLimitWithGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, max_categories INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 1, '2024-01-01T00:00:00.000000Z'),
                    (2, 2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 'A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 'B', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (1, 'C', 30.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 'X', 40.0, '2024-01-01T01:10:00.000000Z'),
                    (2, 'Y', 50.0, '2024-01-01T01:20:00.000000Z'),
                    (2, 'Z', 60.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // order 1: max_categories=1 → top 1 category by total
            // order 2: max_categories=2 → top 2 categories by total
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\ttotal
                            1\tC\t30.0
                            2\tY\t50.0
                            2\tZ\t60.0
                            """,
                    """
                            SELECT o.id, sub.category, sub.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                GROUP BY category
                                ORDER BY total DESC
                                LIMIT o.max_categories
                            ) sub
                            ORDER BY o.id, sub.total
                            """,
                    null, true, false
            );
        });
    }

    // T10: INTERSECT, INNER, equality correlation — per-group intersection
    @Test
    public void testT10Intersect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_a VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:11:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_b VALUES
                    (1, 1, 10.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 1, 30.0, '2024-01-01T00:21:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // order 1: trades_a {10,20} ∩ trades_b {10,30} = {10}
            // order 2: trades_a {30} ∩ trades_b {30} = {30}
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            2\t30.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                INTERSECT
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testT110LargeDatasetLimitPerGroup() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE large_orders AS (
                        SELECT x::INT AS id,
                               ('2024-01-01T00:00:00.000000Z'::TIMESTAMP + x * 3_600_000_000L) AS ts
                        FROM long_sequence(1000)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    CREATE TABLE large_trades AS (
                        SELECT x::INT AS id,
                               ((x - 1) % 1000 + 1)::INT AS order_id,
                               x * 1.5 AS qty,
                               ('2024-01-01T00:00:00.000000Z'::TIMESTAMP + x * 60_000_000L) AS ts
                        FROM long_sequence(10_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            assertQueryNoLeakCheck(
                    """
                            count
                            3000
                            """,
                    """
                            SELECT count(*) AS count
                            FROM large_orders o
                            JOIN LATERAL (
                                SELECT qty FROM large_trades WHERE order_id = o.id ORDER BY qty LIMIT 3
                            ) t
                            """,
                    null, false, true
            );

            // LEFT JOIN with GROUP BY: every order gets a sum
            assertQueryNoLeakCheck(
                    """
                            count
                            1000
                            """,
                    """
                            SELECT count(*) AS count
                            FROM large_orders o
                            LEFT JOIN LATERAL (
                                SELECT sum(qty) AS total FROM large_trades WHERE order_id = o.id
                            ) t
                            """,
                    null, false, true
            );
        });
    }

    @Test
    public void testT110OuterAliasSaveStackTwoBranches() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE fees (order_id INT, fee DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO fees VALUES
                    (1, 0.5, '2024-01-01T00:00:00.000000Z'),
                    (2, 1.0, '2024-01-01T01:00:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tqty\tfee
                            1\t10.0\t0.5
                            1\t20.0\t0.5
                            2\t30.0\t1.0
                            """,
                    """
                            SELECT o.id, sub.qty, sub.fee
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, f.fee
                                FROM (SELECT qty, order_id FROM trades WHERE order_id = o.id) t
                                JOIN (SELECT fee, order_id FROM fees WHERE order_id = o.id) f
                                    ON f.order_id = t.order_id
                            ) sub
                            ORDER BY o.id, sub.qty
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testT110bOuterAliasSaveStackThreeBranches() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE fees (order_id INT, fee DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE discounts (order_id INT, disc DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO fees VALUES
                    (1, 0.5, '2024-01-01T00:00:00.000000Z'),
                    (2, 1.0, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO discounts VALUES
                    (1, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (2, 0.2, '2024-01-01T01:00:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tqty\tfee\tdisc
                            1\t10.0\t0.5\t0.1
                            2\t30.0\t1.0\t0.2
                            """,
                    """
                            SELECT o.id, sub.qty, sub.fee, sub.disc
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, f.fee, d.disc
                                FROM (SELECT qty, order_id FROM trades WHERE order_id = o.id) t
                                JOIN (SELECT fee, order_id FROM fees WHERE order_id = o.id) f
                                    ON f.order_id = t.order_id
                                JOIN (SELECT disc, order_id FROM discounts WHERE order_id = o.id) d
                                    ON d.order_id = t.order_id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testT110cOuterAliasSaveStackMultipleOuterCols() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, category SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, cat SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE limits (order_id INT, cat SYMBOL, max_qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'A', '2024-01-01T00:00:00.000000Z'),
                    (2, 'B', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 'A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 'B', 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO limits VALUES
                    (1, 'A', 100.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 'B', 200.0, '2024-01-01T01:00:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tqty\tmax_qty
                            1\t10.0\t100.0
                            2\t30.0\t200.0
                            """,
                    """
                            SELECT o.id, sub.qty, sub.max_qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, l.max_qty
                                FROM (SELECT qty, order_id FROM trades
                                      WHERE order_id = o.id) t
                                JOIN (SELECT max_qty, order_id FROM limits
                                      WHERE order_id = o.id AND cat = o.category) l
                                    ON l.order_id = t.order_id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testT111LatestByPartitionByCorrelationColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z'),
                    (2, 40.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // PARTITION BY order_id is the same as the correlation column
            // compensateLatestBy should detect it's already present and skip adding
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t20.0
                            2\t40.0
                            """,
                    """
                            SELECT o.id, sub.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty
                                FROM trades
                                WHERE order_id = o.id
                                LATEST ON ts PARTITION BY order_id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T112: Correlated subquery as join branch — triggers decorrelateJoinModelSubqueries
    @Test
    public void testT112CorrelatedSubqueryJoinBranch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, tag_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE tags (id INT, order_id INT, label STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO tags VALUES
                    (1, 1, 'urgent', '2024-01-01T00:05:00.000000Z'),
                    (2, 2, 'normal', '2024-01-01T01:05:00.000000Z')
                    """);

            // JOIN with a correlated subquery: (SELECT ... WHERE order_id = o.id) is a
            // correlated nested model, triggering decorrelateJoinModelSubqueries to
            // clone the __qdb_outer_ref__ into the subquery
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tlabel
                            1\t10.0\turgent
                            2\t30.0\tnormal
                            """,
                    """
                            SELECT o.id, sub.qty, sub.label
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, g.label
                                FROM trades t
                                JOIN (SELECT id, label FROM tags WHERE order_id = o.id) g ON g.id = t.tag_id
                                WHERE t.order_id = o.id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    // T113: LATERAL JOIN without explicit alias — triggers lateralAlias==null path
    @Test
    public void testT113LateralJoinNoAlias() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            1\t20.0
                            2\t30.0
                            3\t40.0
                            3\t50.0
                            """,
                    """
                            SELECT o.id, qty
                            FROM orders o
                            JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id)
                            ORDER BY o.id, qty
                            """,
                    null, true, false
            );
        });
    }

    // T114: 3-branch CASE expression with outer refs — triggers args path
    // (paramCount >= 3 stores children in ExpressionNode.args, not lhs/rhs)
    @Test
    public void testT114CaseExprWithOuterRef() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, lo DOUBLE, hi DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 10.0, 20.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 25.0, 35.0, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 5.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 15.0, '2024-01-01T00:20:00.000000Z'),
                    (1, 25.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 20.0, '2024-01-01T01:10:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:20:00.000000Z'),
                    (2, 40.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // 3-branch CASE (paramCount >= 3 → uses args list)
            // order 1 (lo=10,hi=20): 5→low, 15→mid, 25→high
            // order 2 (lo=25,hi=35): 20→low, 30→mid, 40→high
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tlevel
                            1\t5.0\tlow
                            1\t15.0\tmid
                            1\t25.0\thigh
                            2\t20.0\tlow
                            2\t30.0\tmid
                            2\t40.0\thigh
                            """,
                    """
                            SELECT o.id, sub.qty, sub.level
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty,
                                       CASE WHEN qty < o.lo THEN 'low'
                                            WHEN qty < o.hi THEN 'mid'
                                            ELSE 'high' END AS level
                                FROM trades
                                WHERE order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.qty
                            """,
                    null, true, false
            );
        });
    }

    // T115: LEFT LATERAL with count in COALESCE — triggers wrapCountRefsWithCoalesce args path
    @Test
    public void testT115LeftLateralCountInCoalesce() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // LEFT lateral + count(*): order 3 has no trades → count should be 0
            // CASE wrapping count(*) tests the args path in wrapCountRefsWithCoalesce
            assertQueryNoLeakCheck(
                    """
                            id\tcnt\tlabel
                            1\t2\tmulti
                            2\t1\tsingle
                            3\t0\tnone
                            """,
                    """
                            SELECT o.id, sub.cnt,
                                   CASE
                                       WHEN sub.cnt > 1 THEN 'multi'
                                       WHEN sub.cnt = 1 THEN 'single'
                                       ELSE 'none'
                                   END AS label
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT count(*) AS cnt
                                FROM trades
                                WHERE order_id = o.id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T116: Window function with correlated PARTITION BY inside lateral
    // Triggers hasCorrelatedExprAtDepth window expression args path
    @Test
    public void testT116WindowFunctionCorrelatedPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, category SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'A', '2024-01-01T00:00:00.000000Z'),
                    (2, 'B', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // row_number() OVER (PARTITION BY o.category) — correlated PARTITION BY
            assertQueryNoLeakCheck(
                    """
                            id\tqty\trn
                            1\t10.0\t1
                            1\t20.0\t2
                            2\t30.0\t1
                            """,
                    """
                            SELECT o.id, sub.qty, sub.rn
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty,
                                       row_number() OVER (PARTITION BY o.category ORDER BY ts) AS rn
                                FROM trades
                                WHERE order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.qty
                            """,
                    null, true, true
            );
        });
    }

    // T11: Inner JOIN inside LATERAL, INNER, equality correlation — inner JOIN ON rewrite
    @Test
    public void testT11InnerJoinInLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE products (id INT, order_id INT, name STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE prices (id INT, product_id INT, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO products VALUES
                    (1, 1, 'Widget', '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 'Gadget', '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO prices VALUES
                    (1, 1, 9.99, '2024-01-01T00:20:00.000000Z'),
                    (2, 2, 19.99, '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tname\tprice
                            1\tWidget\t9.99
                            2\tGadget\t19.99
                            """,
                    """
                            SELECT o.id, t.name, t.price
                            FROM orders o
                            JOIN LATERAL (
                                SELECT p.name, pr.price
                                FROM products p
                                JOIN prices pr ON pr.product_id = p.id
                                WHERE p.order_id = o.id
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    // T12: LATEST BY, INNER, equality correlation — PARTITION BY expansion
    @Test
    public void testT12LatestByInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'A', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 'B', 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 'A', 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 'B', 50.0, '2024-01-01T01:20:00.000000Z'),
                    (6, 2, 'B', 60.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // LATEST BY category per order — latest trade in each category per order
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\tqty
                            1\tA\t20.0
                            1\tB\t30.0
                            2\tA\t40.0
                            2\tB\t60.0
                            """,
                    """
                            SELECT o.id, t.category, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, qty FROM trades
                                WHERE order_id = o.id
                                LATEST ON ts PARTITION BY category
                            ) t
                            ORDER BY o.id, t.category
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT13ScanRewritableNonEq() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, min_qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 15.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 25.0, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 20.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 30.0, '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t20.0
                            1\t30.0
                            2\t30.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades
                                WHERE order_id = o.id AND qty > o.min_qty
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T14: Multiple correlation columns, LEFT
    @Test
    public void testT14MultipleCorrelationCols() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (mm_id INT, symbol STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE detail (id INT, mm_id INT, symbol STRING, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO master VALUES
                    (1, 'AAPL', '2024-01-01T00:00:00.000000Z'),
                    (2, 'GOOG', '2024-01-01T01:00:00.000000Z'),
                    (3, 'MSFT', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO detail VALUES
                    (1, 1, 'AAPL', 100.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'AAPL', 200.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, 'GOOG', 300.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            mm_id\tsymbol\ttotal
                            1\tAAPL\t300.0
                            2\tGOOG\t300.0
                            3\tMSFT\tnull
                            """,
                    """
                            SELECT m.mm_id, m.symbol, t.total
                            FROM master m
                            LEFT JOIN LATERAL (
                                SELECT sum(qty) AS total FROM detail
                                WHERE mm_id = m.mm_id AND symbol = m.symbol
                            ) t
                            ORDER BY m.mm_id
                            """,
                    null, true, false
            );
        });
    }

    // T15: Empty result set, LEFT — outer rows preserved with NULLs
    @Test
    public void testT15EmptyResultLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            // no trades at all

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\tnull
                            2\tnull
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            LEFT JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT17LimitAndWindowLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 50.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Window function + LIMIT — LEFT JOIN so order 3 gets NULLs
            assertQueryNoLeakCheck(
                    """
                            id\tqty\trunning_sum
                            1\t30.0\t30.0
                            1\t20.0\t50.0
                            2\t50.0\t50.0
                            2\t40.0\t90.0
                            3\tnull\tnull
                            """,
                    """
                            SELECT o.id, t.qty, t.running_sum
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT qty, sum(qty) OVER (ORDER BY qty DESC) AS running_sum
                                FROM trades
                                WHERE order_id = o.id
                                ORDER BY qty DESC
                                LIMIT 2
                            ) t
                            ORDER BY o.id, t.qty DESC
                            """,
                    null, true, false
            );
        });
    }

    // T18: NULL join key, INNER — NULL-safe join semantics
    @Test
    public void testT18NullJoinKeyInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, null, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // QuestDB Hash Join is NULL-safe: NULL = NULL matches
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            null\t20.0
                            1\t10.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T19: NULL join key, LEFT — LEFT JOIN + NULL key
    @Test
    public void testT19NullJoinKeyLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (2, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, null, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // LEFT JOIN: order 2 has no trades → NULL fill
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            null\t20.0
                            1\t10.0
                            2\tnull
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            LEFT JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T20: No correlation — degrades to CROSS JOIN
    @Test
    public void testT20UncorrelatedLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 20.0, '2024-01-01T01:30:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            1\t20.0
                            2\t10.0
                            2\t20.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            LATERAL (SELECT qty FROM trades) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, true
            );
        });
    }

    // T21: Non-constant LIMIT
    @Test
    public void testT21CorrelatedLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, n INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 2, '2024-01-01T00:00:00.000000Z'),
                    (2, 1, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 50.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Correlated LIMIT: order 1 gets 2 rows, order 2 gets 1 row
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            1\t20.0
                            2\t40.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id ORDER BY qty LIMIT o.n) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT21bCorrelatedOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, n INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 1, '2024-01-01T00:00:00.000000Z'),
                    (2, 0, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 50.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Correlated offset: order 1 skips 1 row (gets rows 2-3), order 2 skips 0 (gets rows 1-3)
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t20.0
                            1\t30.0
                            2\t40.0
                            2\t50.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id ORDER BY qty LIMIT o.n, 5) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT22NestedLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (id INT, t1_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t3 (id INT, t2_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t1 VALUES (1, '2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO t2 VALUES (1, 1, '2024-01-01T00:30:00.000000Z')");
            execute("INSERT INTO t3 VALUES (1, 1, '2024-01-01T01:00:00.000000Z')");

            assertQueryNoLeakCheck(
                    """
                            id\tid1\tt1_id\tts1\tid2\tt2_id\tts2
                            1\t1\t1\t2024-01-01T00:30:00.000000Z\t1\t1\t2024-01-01T01:00:00.000000Z
                            """,
                    """
                            SELECT t1.id, sub1.*
                            FROM t1
                            JOIN LATERAL (
                                SELECT t2.id AS id1, t2.t1_id, t2.ts AS ts1,
                                       sub2.id AS id2, sub2.t2_id, sub2.ts AS ts2
                                FROM t2
                                JOIN LATERAL (SELECT * FROM t3 WHERE t3.t2_id = t2.id) sub2 ON 1 = 1
                                WHERE t2.t1_id = t1.id
                            ) sub1
                            """,
                    null, false, true
            );
        });
    }

    // T23: SAMPLE BY (rewritten to GROUP BY), INNER — decorrelation after rewriteSampleBy
    @Test
    public void testT23SampleByInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:40:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T01:10:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 50.0, '2024-01-01T01:40:00.000000Z')
                    """);

            // SAMPLE BY is rewritten to GROUP BY before LATERAL decorrelation
            assertQueryNoLeakCheck(
                    """
                            id\tts\ttotal
                            1\t2024-01-01T00:00:00.000000Z\t10.0
                            1\t2024-01-01T00:30:00.000000Z\t20.0
                            1\t2024-01-01T01:00:00.000000Z\t30.0
                            2\t2024-01-01T01:00:00.000000Z\t40.0
                            2\t2024-01-01T01:30:00.000000Z\t50.0
                            """,
                    """
                            SELECT o.id, t.ts, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT ts, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                SAMPLE BY 30m
                            ) t
                            ORDER BY o.id, t.ts
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT23bKeyedSampleByInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'B', 20.0, '2024-01-01T00:10:00.000000Z'),
                    (3, 1, 'A', 30.0, '2024-01-01T00:40:00.000000Z'),
                    (4, 2, 'A', 40.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id	category	ts	total
                            1	A	2024-01-01T00:00:00.000000Z	10.0
                            1	A	2024-01-01T00:30:00.000000Z	30.0
                            1	A	2024-01-01T01:00:00.000000Z	30.0
                            1	B	2024-01-01T00:00:00.000000Z	20.0
                            1	B	2024-01-01T00:30:00.000000Z	20.0
                            1	B	2024-01-01T01:00:00.000000Z	20.0
                            2	A	2024-01-01T00:00:00.000000Z	null
                            2	A	2024-01-01T00:30:00.000000Z	null
                            2	A	2024-01-01T01:00:00.000000Z	40.0
                            """,
                    """
                            SELECT o.id, t.category, t.ts, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, ts, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                SAMPLE BY 30m FILL(PREV)
                            ) t
                            ORDER BY o.id, t.category, t.ts
                            """,
                    null, true, true
            );
        });
    }

    // T23c: UNION (distinct), INNER, equality correlation — per-group dedup
    @Test
    public void testT23cUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_a VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_b VALUES
                    (1, 1, 10.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 1, 30.0, '2024-01-01T00:21:00.000000Z'),
                    (3, 2, 20.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // order 1: trades_a {10} ∪ trades_b {10,30} = {10,30} (10 deduped)
            // order 2: trades_a {20} ∪ trades_b {20} = {20} (20 deduped)
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            1\t30.0
                            2\t20.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                UNION
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, true
            );
        });
    }

    // T25: Multi-level nested subquery (non-nested correlation)
    @Test
    public void testT25MultiLevelSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // Inner subquery wraps the correlated query in another layer — only outer correlation, no nesting
            assertQueryNoLeakCheck(
                    """
                            id\tmax_qty
                            1\t20.0
                            2\t30.0
                            """,
                    """
                            SELECT o.id, t.max_qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT max(qty) AS max_qty
                                FROM trades
                                WHERE order_id = o.id
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T26: ORDER BY outer ref (no LIMIT) — ORDER BY expression rewrite
    @Test
    public void testT26OrderByOuterRef() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, priority INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 2, '2024-01-01T00:00:00.000000Z'),
                    (2, 1, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            2\t20.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades WHERE order_id = o.id
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT27GroupByOuterRef() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, category STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'X', '2024-01-01T00:00:00.000000Z'),
                    (2, 'Y', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // GROUP BY includes outer ref o.category — rewriteGroupByExpressions rewrites it
            assertQueryNoLeakCheck(
                    """
                            id	cat	total
                            1	X	30.0
                            2	Y	30.0
                            """,
                    """
                            SELECT o.id, t.cat, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT o.category AS cat, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                GROUP BY o.category
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T28: PIVOT inside LATERAL — rewritePivot already converted to GROUP BY
    @Test
    public void testT28PivotInsideLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, side SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'buy', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'sell', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 'buy', 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 'sell', 40.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // PIVOT is rewritten before LATERAL decorrelation
            assertQueryNoLeakCheck(
                    """
                            id\tbuy\tsell
                            1\t40.0\t20.0
                            2\tnull\t40.0
                            """,
                    """
                            SELECT o.id, t.buy, t.sell
                            FROM orders o
                            JOIN LATERAL (
                                SELECT * FROM (
                                    SELECT side, sum(qty) AS total
                                    FROM trades
                                    WHERE order_id = o.id
                                    GROUP BY side
                                ) PIVOT (sum(total) FOR side IN ('buy', 'sell'))
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T28b: PIVOT inside LATERAL, LEFT — NULL fill for unmatched outer rows
    @Test
    public void testT28bPivotInsideLateralLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, side SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'buy', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'sell', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, 'buy', 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // order 3 has no trades → LEFT fills NULLs
            assertQueryNoLeakCheck(
                    """
                            id\tbuy\tsell
                            1\t10.0\t20.0
                            2\t30.0\tnull
                            3\tnull\tnull
                            """,
                    """
                            SELECT o.id, t.buy, t.sell
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT * FROM (
                                    SELECT side, sum(qty) AS total
                                    FROM trades
                                    WHERE order_id = o.id
                                    GROUP BY side
                                ) PIVOT (sum(total) FOR side IN ('buy', 'sell'))
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT28c2SampleByCountLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:40:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // SAMPLE BY with count: order 3 has no trades → LEFT JOIN fills count with 0
            assertQueryNoLeakCheck(
                    """
                            id\tts\tcnt\ttotal
                            1\t2024-01-01T00:00:00.000000Z\t1\t10.0
                            1\t2024-01-01T00:30:00.000000Z\t1\t20.0
                            2\t2024-01-01T01:00:00.000000Z\t1\t30.0
                            3\t\t0\tnull
                            """,
                    """
                            SELECT o.id, t.ts, t.cnt, t.total
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT ts, count(*) AS cnt, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                SAMPLE BY 30m
                            ) t
                            ORDER BY o.id, t.ts
                            """,
                    null, true, false
            );
        });
    }

    // T28c: SAMPLE BY + LEFT JOIN — NULL fill for unmatched outer rows
    @Test
    public void testT28cSampleByLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:40:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // SAMPLE BY rewritten to GROUP BY; order 3 has no trades → LEFT fills NULLs
            assertQueryNoLeakCheck(
                    """
                            id	ts	total
                            1	2024-01-01T00:00:00.000000Z	10.0
                            1	2024-01-01T00:30:00.000000Z	20.0
                            2	2024-01-01T01:00:00.000000Z	30.0
                            3		null
                            """,
                    """
                            SELECT o.id, t.ts, t.total
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT ts, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                SAMPLE BY 30m
                            ) t
                            ORDER BY o.id, t.ts
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT28dSampleByAlignToFirstObservation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:05:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:35:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:12:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:42:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id	ts	total
                            1	2024-01-01T00:05:00.000000Z	10.0
                            1	2024-01-01T00:35:00.000000Z	20.0
                            2	2024-01-01T01:05:00.000000Z	30.0
                            2	2024-01-01T01:35:00.000000Z	40.0
                            """,
                    """
                            SELECT o.id, t.ts, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT ts, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                SAMPLE BY 30m ALIGN TO FIRST OBSERVATION
                            ) t
                            ORDER BY o.id, t.ts
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testT28eSampleByFillLinear() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 1, 30.0, '2024-01-01T01:00:00.000000Z'),
                    (3, 2, 100.0, '2024-01-01T01:00:00.000000Z'),
                    (4, 2, 200.0, '2024-01-01T02:00:00.000000Z')
                    """);

            // FILL(LINEAR): fills missing buckets with linear interpolation
            // order 1: [00:00]=10, [00:30]=20(filled), [01:00]=30
            // order 2: [01:00]=100, [01:30]=150(filled), [02:00]=200
            assertQueryNoLeakCheck(
                    """
                            id	ts	total
                            1	2024-01-01T00:00:00.000000Z	10.0
                            1	2024-01-01T00:30:00.000000Z	20.0
                            1	2024-01-01T01:00:00.000000Z	30.0
                            1	2024-01-01T01:30:00.000000Z	40.0
                            1	2024-01-01T02:00:00.000000Z	50.0
                            2	2024-01-01T00:00:00.000000Z	0.0
                            2	2024-01-01T00:30:00.000000Z	50.0
                            2	2024-01-01T01:00:00.000000Z	100.0
                            2	2024-01-01T01:30:00.000000Z	150.0
                            2	2024-01-01T02:00:00.000000Z	200.0
                            """,
                    """
                            SELECT o.id, t.ts, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT ts, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                SAMPLE BY 30m FILL(LINEAR)
                            ) t
                            ORDER BY o.id, t.ts
                            """,
                    null, true, false
            );
        });
    }

    // T28f: SAMPLE BY + LIMIT — combined wrappers: rewriteSampleBy then compensateLimit
    @Test
    public void testT28fSampleByWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:40:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T01:10:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 50.0, '2024-01-01T01:40:00.000000Z'),
                    (6, 2, 60.0, '2024-01-01T02:10:00.000000Z')
                    """);

            // SAMPLE BY + LIMIT 2: take the first 2 time buckets per order
            assertQueryNoLeakCheck(
                    """
                            id\tts\ttotal
                            1\t2024-01-01T00:00:00.000000Z\t10.0
                            1\t2024-01-01T00:30:00.000000Z\t20.0
                            2\t2024-01-01T01:00:00.000000Z\t40.0
                            2\t2024-01-01T01:30:00.000000Z\t50.0
                            """,
                    """
                            SELECT o.id, t.ts, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT ts, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                SAMPLE BY 30m
                                LIMIT 2
                            ) t
                            ORDER BY o.id, t.ts
                            """,
                    null, true, false
            );
        });
    }

    // T28g: ORDER BY with function expression — moveOrderByFunctionsIntoOuterSelect wrapper
    @Test
    public void testT28gOrderByFunctionWrapper() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 30.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 10.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 20.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 50.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 40.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // ORDER BY abs(qty - 25) triggers moveOrderByFunctionsIntoOuterSelect
            // which wraps the inner model with a SELECT * wrapper
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t30.0
                            1\t20.0
                            1\t10.0
                            2\t50.0
                            2\t40.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades
                                WHERE order_id = o.id
                                ORDER BY abs(qty - 25) DESC
                            ) t
                            ORDER BY o.id, t.qty DESC
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT28hPivotMultiCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (mm_id INT, symbol STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE fills (id INT, mm_id INT, symbol STRING, side SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO master VALUES
                    (1, 'AAPL', '2024-01-01T00:00:00.000000Z'),
                    (2, 'GOOG', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO fills VALUES
                    (1, 1, 'AAPL', 'buy', 100.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'AAPL', 'sell', 50.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, 'GOOG', 'buy', 200.0, '2024-01-01T01:10:00.000000Z'),
                    (4, 2, 'GOOG', 'sell', 300.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Two equality correlation columns: mm_id AND symbol
            assertQueryNoLeakCheck(
                    """
                            mm_id\tsymbol\tbuy\tsell
                            1\tAAPL\t100.0\t50.0
                            2\tGOOG\t200.0\t300.0
                            """,
                    """
                            SELECT m.mm_id, m.symbol, t.buy, t.sell
                            FROM master m
                            JOIN LATERAL (
                                SELECT * FROM (
                                    SELECT side, sum(qty) AS total
                                    FROM fills
                                    WHERE mm_id = m.mm_id AND symbol = m.symbol
                                    GROUP BY side
                                ) PIVOT (sum(total) FOR side IN ('buy', 'sell'))
                            ) t
                            ORDER BY m.mm_id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT28iGroupByWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, category STRING, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'A', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 'B', 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 1, 'C', 5.0, '2024-01-01T00:40:00.000000Z'),
                    (5, 2, 'X', 50.0, '2024-01-01T01:10:00.000000Z'),
                    (6, 2, 'Y', 60.0, '2024-01-01T01:20:00.000000Z'),
                    (7, 2, 'Z', 70.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // GROUP BY category + LIMIT 2: top 2 categories by total per order
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\ttotal
                            1\tB\t30.0
                            1\tA\t30.0
                            2\tZ\t70.0
                            2\tY\t60.0
                            """,
                    """
                            SELECT o.id, t.category, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                GROUP BY category
                                ORDER BY total DESC
                                LIMIT 2
                            ) t
                            ORDER BY o.id, t.total DESC
                            """,
                    null, true, false
            );

            // implicit aggregation
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\ttotal
                            1\tB\t30.0
                            1\tA\t30.0
                            2\tZ\t70.0
                            2\tY\t60.0
                            """,
                    """
                            SELECT o.id, t.category, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                ORDER BY total DESC
                                LIMIT 2
                            ) t
                            ORDER BY o.id, t.total DESC
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT28jDistinctWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, category STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'A', '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'A', '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 'B', '2024-01-01T00:30:00.000000Z'),
                    (4, 1, 'C', '2024-01-01T00:40:00.000000Z'),
                    (5, 2, 'X', '2024-01-01T01:10:00.000000Z'),
                    (6, 2, 'X', '2024-01-01T01:20:00.000000Z'),
                    (7, 2, 'Y', '2024-01-01T01:30:00.000000Z')
                    """);

            // DISTINCT + LIMIT 2: first 2 distinct categories per order
            assertQueryNoLeakCheck(
                    """
                            id\tcategory
                            1\tA
                            1\tB
                            2\tX
                            2\tY
                            """,
                    """
                            SELECT o.id, t.category
                            FROM orders o
                            JOIN LATERAL (
                                SELECT DISTINCT category FROM trades
                                WHERE order_id = o.id
                                ORDER BY category
                                LIMIT 2
                            ) t
                            ORDER BY o.id, t.category
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT29CteInsideLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\ttotal
                            1\t30.0
                            2\t30.0
                            """,
                    """
                            SELECT o.id, t.total
                            FROM orders o
                            JOIN LATERAL (
                                WITH matched AS (
                                    SELECT qty FROM trades WHERE order_id = o.id
                                )
                                SELECT sum(qty) AS total FROM matched
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T30: ungrouped count(*), INNER — decorrelation adds GROUP BY, unmatched rows dropped
    @Test
    public void testT30UngroupedCountInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, '2024-01-01T01:10:00.000000Z')
                    """);

            // order 3 has no trades → INNER JOIN drops it
            assertQueryNoLeakCheck(
                    """
                            id\tcnt
                            1\t2
                            2\t1
                            """,
                    """
                            SELECT o.id, t.cnt
                            FROM orders o
                            JOIN LATERAL (SELECT count(*) AS cnt FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T31: ungrouped count(*), LEFT — no match returns 0 (not NULL)
    @Test
    public void testT31UngroupedCountLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, '2024-01-01T00:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tcnt
                            1\t2
                            2\t0
                            3\t0
                            """,
                    """
                            SELECT o.id, t.cnt
                            FROM orders o
                            LEFT JOIN LATERAL (SELECT count(*) AS cnt FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T32: ungrouped sum(), LEFT — no match returns NULL (sum's correct semantics)
    @Test
    public void testT32UngroupedSumLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\ttotal
                            1\t30.0
                            2\tnull
                            3\tnull
                            """,
                    """
                            SELECT o.id, t.total
                            FROM orders o
                            LEFT JOIN LATERAL (SELECT sum(qty) AS total FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T34: Scan + non-rewritable non-eq (no aggregate) → postJoinWhereClause
    @Test
    public void testT34ScanNonRewritableNonEqSimple() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, start_ts TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:15:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:15:00.000000Z', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 50.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // o.start_ts is not in eq map (only o.id is)
            // No aggregate → simple scan path → postJoinWhereClause
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t20.0
                            1\t30.0
                            2\t50.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty, ts AS trade_ts FROM trades
                                WHERE order_id = o.id AND ts > o.start_ts
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T35: Complex equality — outer side is expression (inner.col = f(outer.col))
    @Test
    public void testT35ComplexEquality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, group_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (10, '2024-01-01T00:00:00.000000Z'),
                    (20, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 100.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 200.0, '2024-01-01T01:10:00.000000Z'),
                    (3, 1, 300.0, '2024-01-01T00:20:00.000000Z')
                    """);

            // group_id = o.id / 10 → outer side is expression
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            10\t100.0
                            10\t300.0
                            20\t200.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades WHERE group_id = o.id / 10
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, true
            );
        });
    }

    // T36: Inner JOIN ON correlation — extractCorrelatedFromInnerJoins
    @Test
    public void testT36InnerJoinOnCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t1 (id INT, val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (id INT, order_id INT, t1_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("INSERT INTO t1 VALUES (1, 'X', '2024-01-01T00:10:00.000000Z'), (2, 'Y', '2024-01-01T01:10:00.000000Z')");
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 1, 1, '2024-01-01T00:20:00.000000Z'),
                    (2, 2, 2, '2024-01-01T01:20:00.000000Z')
                    """);

            // Correlation is only in the inner JOIN ON clause: t2.order_id = o.id
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\tX
                            2\tY
                            """,
                    """
                            SELECT o.id, sub.val
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t1.val
                                FROM t1
                                JOIN t2 ON t2.t1_id = t1.id AND t2.order_id = o.id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    // T37: GROUP BY + mixed eq + rewritable non-eq
    @Test
    public void testT37GroupByRewritableNonEq() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, min_qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 15.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 25.0, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 20.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 30.0, '2024-01-01T01:20:00.000000Z'),
                    (6, 2, 40.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // eq: order_id = o.id; non-eq: qty > o.min_qty (rewritable since o.id maps min_qty too)
            // Well actually o.min_qty is NOT in the alias map, but it's a simple scan + aggregate scenario
            // with non-rewritable non-eq → Delim Scan path
            assertQueryNoLeakCheck(
                    """
                            id\ttotal
                            1\t50.0
                            2\t70.0
                            """,
                    """
                            SELECT o.id, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT sum(qty) AS total FROM trades
                                WHERE order_id = o.id AND qty > o.min_qty
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T38: OR predicate with correlated ref — no equality predicates, simple scan
    @Test
    public void testT38OrCorrelatedPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQueryNoLeakCheck(
                    """
                            id\tcustomer\tqty
                            1\tAlice\t10.0
                            1\tAlice\t20.0
                            2\tBob\t30.0
                            3\tCharlie\t40.0
                            3\tCharlie\t50.0
                            """,
                    """
                            SELECT o.id, o.customer, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades
                                WHERE order_id = o.id OR (qty > 100.0 AND order_id = o.id)
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, true
            );
        });
    }

    // T39: Correlated ref only in SELECT expression — Delim needed
    @Test
    public void testT39CorrelatedRefInSelect() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQueryNoLeakCheck(
                    """
                            id\tqty_plus_id
                            1\t11.0
                            1\t21.0
                            2\t32.0
                            3\t43.0
                            3\t53.0
                            """,
                    """
                            SELECT o.id, t.qty_plus_id
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty + o.id AS qty_plus_id FROM trades
                                WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty_plus_id
                            """,
                    null, true, false
            );
        });
    }

    // T40: Correlated ref in SELECT with aggregate — Delim needed
    @Test
    public void testT40CorrelatedRefInSelectWithAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQueryNoLeakCheck(
                    """
                            id\ttotal_plus_id
                            1\t31.0
                            2\t32.0
                            3\t93.0
                            """,
                    """
                            SELECT o.id, t.total_plus_id
                            FROM orders o
                            JOIN LATERAL (
                                SELECT sum(qty) + o.id AS total_plus_id FROM trades
                                WHERE order_id = o.id
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T41: No equality predicates at all, only non-eq correlated WHERE
    @Test
    public void testT41OnlyNonEqCorrelatedPredicate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sensors (id INT, threshold DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE readings (sensor_id INT, value DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO sensors VALUES
                    (1, 50.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO readings VALUES
                    (1, 60.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 40.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 35.0, '2024-01-01T01:10:00.000000Z'),
                    (2, 25.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Only non-eq correlated pred: value > s.threshold AND sensor_id = s.id
            // sensor_id = s.id is eq, value > s.threshold is non-eq
            assertQueryNoLeakCheck(
                    """
                            id\tvalue
                            1\t60.0
                            2\t35.0
                            """,
                    """
                            SELECT s.id, r.value
                            FROM sensors s
                            JOIN LATERAL (
                                SELECT value FROM readings
                                WHERE sensor_id = s.id AND value > s.threshold
                            ) r
                            ORDER BY s.id
                            """,
                    null, true, false
            );
        });
    }

    // T42: Nested LATERAL - inner LATERAL references both outer-outer and direct outer columns
    @Test
    public void testT42NestedLateralBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t3 (c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (10, '2024-01-01T00:10:00.000000Z'),
                    (20, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t3 VALUES
                    (100, '2024-01-01T00:20:00.000000Z'),
                    (200, '2024-01-01T01:20:00.000000Z')
                    """);

            // inner LATERAL SELECT references t1.a (outer-outer) and t2.b (direct outer)
            assertQueryNoLeakCheck(
                    """
                            a\tb\tresult
                            1\t10\t111
                            1\t20\t121
                            2\t10\t112
                            2\t20\t122
                            """,
                    """
                            SELECT t1.a, x.b, x.result
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT t2.b, y.result
                                FROM t2
                                CROSS JOIN LATERAL (
                                    SELECT t1.a + t2.b + t3.c AS result FROM t3
                                    LIMIT 1
                                ) y
                            ) x
                            ORDER BY t1.a, x.b
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testT43NestedLateralWithCorrelatedWhere() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE departments (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE employees (dept_id INT, name STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE tasks (emp_name STRING, dept_id INT, priority INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO departments VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO employees VALUES
                    (1, 'Alice', '2024-01-01T00:10:00.000000Z'),
                    (1, 'Bob', '2024-01-01T00:20:00.000000Z'),
                    (2, 'Charlie', '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO tasks VALUES
                    ('Alice', 1, 5, '2024-01-01T00:30:00.000000Z'),
                    ('Bob', 1, 3, '2024-01-01T00:40:00.000000Z'),
                    ('Charlie', 2, 7, '2024-01-01T01:30:00.000000Z')
                    """);

            // Inner LATERAL WHERE uses dept_id = d.id (outer-outer ref)
            assertQueryNoLeakCheck(
                    """
                            id\tname\tpriority
                            1\tAlice\t5
                            1\tBob\t3
                            2\tCharlie\t7
                            """,
                    """
                            SELECT d.id, x.name, x.priority
                            FROM departments d
                            JOIN LATERAL (
                                SELECT e.name, t.priority
                                FROM employees e
                                JOIN LATERAL (
                                    SELECT priority FROM tasks
                                    WHERE emp_name = e.name AND dept_id = d.id
                                ) t ON 1 = 1
                                WHERE e.dept_id = d.id
                            ) x
                            ORDER BY d.id, x.name
                            """,
                    null, true, true
            );
        });
    }

    // T44: Nested LATERAL with aggregate in inner
    @Test
    public void testT44NestedLateralWithAggregate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE categories (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE products (cat_id INT, name STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE sales (product_name STRING, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO categories VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO products VALUES
                    (1, 'Widget', '2024-01-01T00:10:00.000000Z'),
                    (1, 'Gadget', '2024-01-01T00:20:00.000000Z'),
                    (2, 'Doohickey', '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO sales VALUES
                    ('Widget', 100.0, '2024-01-01T00:30:00.000000Z'),
                    ('Widget', 150.0, '2024-01-01T00:40:00.000000Z'),
                    ('Gadget', 200.0, '2024-01-01T00:50:00.000000Z'),
                    ('Doohickey', 300.0, '2024-01-01T01:30:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tname\ttotal
                            1\tGadget\t200.0
                            1\tWidget\t250.0
                            2\tDoohickey\t300.0
                            """,
                    """
                            SELECT c.id, x.name, x.total
                            FROM categories c
                            JOIN LATERAL (
                                SELECT p.name, s.total
                                FROM products p
                                JOIN LATERAL (
                                    SELECT sum(amount) AS total FROM sales
                                    WHERE product_name = p.name
                                ) s ON 1 = 1
                                WHERE p.cat_id = c.id
                            ) x
                            ORDER BY c.id, x.name
                            """,
                    null, true, true
            );
        });
    }

    // T45: Nested LATERAL - outer-outer ref only in SELECT expression of innermost subquery
    @Test
    public void testT45NestedLateralOuterRefOnlyInSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (10, '2024-01-01T00:00:00.000000Z'),
                    (20, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, '2024-01-01T00:10:00.000000Z'),
                    (2, '2024-01-01T01:10:00.000000Z')
                    """);

            // Inner LATERAL's SELECT references t1.a (outer-outer) without any WHERE correlation
            assertQueryNoLeakCheck(
                    """
                            a\tb\tresult
                            10\t1\t11
                            10\t2\t12
                            20\t1\t21
                            20\t2\t22
                            """,
                    """
                            SELECT t1.a, x.b, x.result
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT t2.b, y.result
                                FROM t2
                                CROSS JOIN LATERAL (
                                    SELECT t1.a + t2.b AS result
                                ) y
                            ) x
                            ORDER BY t1.a, x.b
                            """,
                    null, true, true
            );
        });
    }

    // T46: EXCEPT, INNER, equality correlation — per-group set difference
    @Test
    public void testT46Except() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_a VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:11:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:11:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_b VALUES
                    (1, 1, 10.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 2, 30.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // order 1: trades_a {10,20} \ trades_b {10} = {20}
            // order 2: trades_a {30,40} \ trades_b {30} = {40}
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t20.0
                            2\t40.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                EXCEPT
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, true
            );
        });
    }

    // T47: INTERSECT ALL, INNER, equality correlation — preserves duplicates
    @Test
    public void testT47IntersectAll() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_a VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 10.0, '2024-01-01T00:11:00.000000Z'),
                    (3, 1, 20.0, '2024-01-01T00:12:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_b VALUES
                    (1, 1, 10.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 1, 10.0, '2024-01-01T00:21:00.000000Z'),
                    (3, 1, 10.0, '2024-01-01T00:22:00.000000Z')
                    """);

            // order 1: trades_a {10,10,20} INTERSECT ALL trades_b {10,10,10} = {10,10}
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            1\t10.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                INTERSECT ALL
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testT48ExceptAll() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_a VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 10.0, '2024-01-01T00:11:00.000000Z'),
                    (3, 1, 10.0, '2024-01-01T00:12:00.000000Z'),
                    (4, 1, 20.0, '2024-01-01T00:13:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_b VALUES
                    (1, 1, 10.0, '2024-01-01T00:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t20.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                EXCEPT ALL
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, true
            );
        });
    }

    // T49: EXCEPT with LEFT LATERAL — unmatched outer rows preserved
    @Test
    public void testT49ExceptLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_a VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:11:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_b VALUES
                    (1, 1, 10.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 2, 30.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // order 1: trades_a {10,20} \ trades_b {10} = {20}
            // order 2: trades_a {30} \ trades_b {30} = {} → LEFT fills NaN
            // order 3: no trades → LEFT fills NaN
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t20.0
                            2\tnull
                            3\tnull
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                EXCEPT
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T50: UNION with multi-group cross-group correctness — verifies no cross-group interference
    @Test
    public void testT50UnionMultiGroup() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            // Same qty=10 appears in both branches for different orders
            execute("""
                    INSERT INTO trades_a VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 10.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_b VALUES
                    (1, 1, 10.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 3, 10.0, '2024-01-01T02:20:00.000000Z')
                    """);

            // UNION dedup must be per-group, not global:
            // order 1: {10} ∪ {10} = {10} (deduped within group)
            // order 2: {10} ∪ {} = {10}
            // order 3: {} ∪ {10} = {10}
            // All three orders produce qty=10, but they must NOT be deduped across groups
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            2\t10.0
                            3\t10.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                UNION
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, true
            );
        });
    }

    // T51: Wrapper-layer correlation — outer ref only in wrapper SELECT, content has equality correlation
    @Test
    public void testT51WrapperSelectCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (id INT, t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (10, 100, '2024-01-01T00:00:00.000000Z'),
                    (20, 200, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, 1, '2024-01-01T00:10:00.000000Z'),
                    (2, 10, 2, '2024-01-01T00:20:00.000000Z'),
                    (3, 20, 3, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            a\tresult
                            10\t101
                            10\t102
                            20\t203
                            """,
                    """
                            SELECT t1.a, x.result
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT val + t1.b AS result
                                FROM (
                                    SELECT val FROM t2 WHERE t2.t1_id = t1.a
                                )
                            ) x
                            ORDER BY t1.a, x.result
                            """,
                    null, true, false
            );
        });
    }

    // T52: Wrapper-layer correlation — outer ref in wrapper WHERE, content has equality correlation
    @Test
    public void testT52WrapperWhereCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, threshold INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 5, '2024-01-01T00:00:00.000000Z'),
                    (2, 25, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 3, '2024-01-01T00:10:00.000000Z'),
                    (1, 10, '2024-01-01T00:20:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z'),
                    (2, 30, '2024-01-01T01:20:00.000000Z')
                    """);

            // Wrapper WHERE references t1.threshold (outer), content WHERE has equality t2.t1_id = t1.a
            assertQueryNoLeakCheck(
                    """
                            a\tval
                            1\t10
                            2\t30
                            """,
                    """
                            SELECT t1.a, x.val
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT val FROM (
                                    SELECT val FROM t2 WHERE t2.t1_id = t1.a
                                )
                                WHERE val > t1.threshold
                            ) x
                            ORDER BY t1.a
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT53WrapperSelectAndWhereCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, b INT, threshold INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 100, 5, '2024-01-01T00:00:00.000000Z'),
                    (2, 200, 25, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 3, '2024-01-01T00:10:00.000000Z'),
                    (1, 10, '2024-01-01T00:20:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z'),
                    (2, 30, '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            a\tresult
                            1\t110
                            2\t230
                            """,
                    """
                            SELECT t1.a, x.result
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT val + t1.b AS result
                                FROM (
                                    SELECT val FROM t2 WHERE t2.t1_id = t1.a
                                )
                                WHERE val > t1.threshold
                            ) x
                            ORDER BY t1.a
                            """,
                    null, true, false
            );

            assertQueryNoLeakCheck(
                    """
                            a	result1	result
                            1	111	110
                            2	231	230
                            """,
                    """
                            SELECT t1.a, x.result1, x.result
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT val + t1.b AS result, val + t1.b + 1 AS result1
                                FROM (
                                    SELECT val FROM t2 WHERE t2.t1_id = t1.a
                                )
                                WHERE val > t1.threshold
                            ) x
                            ORDER BY t1.a
                            """,
                    null, true, false
            );


            assertQueryNoLeakCheck(
                    """
                            a	column
                            1	1
                            2	1
                            """,
                    """
                            SELECT t1.a, x.result1 - x.result
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT val + t1.b AS result, val + t1.b + 1 AS result1
                                FROM (
                                    SELECT val FROM t2 WHERE t2.t1_id = t1.a
                                )
                                WHERE val > t1.threshold
                            ) x
                            ORDER BY t1.a
                            """,
                    null, true, false
            );

            assertQueryNoLeakCheck(
                    """
                            a	column
                            2	231
                            """,
                    """
                            SELECT t1.a, x.result + 1
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT val + t1.b AS result, val + t1.b + 1 AS result1
                                FROM (
                                    SELECT val FROM t2 WHERE t2.t1_id = t1.a
                                )
                                WHERE val > t1.threshold and val > 10
                            ) x
                            ORDER BY t1.a
                            """,
                    null, true, false
            );

            assertQueryNoLeakCheck(
                    """
                            a	b
                            1	100
                            1	100
                            2	200
                            """,
                    """
                            SELECT t1.a, x.b
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT t1.b as b
                                FROM (
                                    SELECT val FROM t2 WHERE t2.t1_id = t1.a
                                )
                                WHERE val > t1.threshold or val < 10
                            ) x
                            ORDER BY t1.a
                            """,
                    null, true, false
            );
        });
    }

    // T54: Wrapper-layer correlation with aggregate in content
    @Test
    public void testT54WrapperCorrelationWithAggregate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, multiplier INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 10, '2024-01-01T00:00:00.000000Z'),
                    (2, 20, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 3, '2024-01-01T00:10:00.000000Z'),
                    (1, 7, '2024-01-01T00:20:00.000000Z'),
                    (2, 5, '2024-01-01T01:10:00.000000Z'),
                    (2, 15, '2024-01-01T01:20:00.000000Z')
                    """);

            // Wrapper SELECT references t1.multiplier, content has aggregate SUM + equality
            assertQueryNoLeakCheck(
                    """
                            a\tresult
                            1\t100
                            2\t400
                            """,
                    """
                            SELECT t1.a, x.result
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT total * t1.multiplier AS result
                                FROM (
                                    SELECT SUM(val) AS total FROM t2 WHERE t2.t1_id = t1.a
                                )
                            ) x
                            ORDER BY t1.a
                            """,
                    null, true, false
            );
        });
    }

    // T55: Subquery outer with non-equality correlation — __outer_ref must preserve outer WHERE
    @Test
    public void testT55SubqueryOuterNonEqCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, status STRING, total DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE items (order_id INT, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'active', 50.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 'closed', 100.0, '2024-01-01T01:00:00.000000Z'),
                    (3, 'active', 200.0, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO items VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 60.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 110.0, '2024-01-01T01:10:00.000000Z'),
                    (3, 50.0, '2024-01-01T02:10:00.000000Z'),
                    (3, 250.0, '2024-01-01T02:20:00.000000Z')
                    """);

            // Outer is a subquery with WHERE status = 'active' (filters out order 2).
            // Non-equality correlation: items.price > o.total.
            // __outer_ref must preserve the WHERE — without it, order 2's total=100
            // would leak into __outer_ref and produce wrong results.
            assertQueryNoLeakCheck(
                    """
                            id\tprice
                            1\t60.0
                            3\t250.0
                            """,
                    """
                            SELECT o.id, t.price
                            FROM (SELECT * FROM orders WHERE status = 'active') o
                            CROSS JOIN LATERAL (
                                SELECT price FROM items
                                WHERE order_id = o.id AND price > o.total
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT56LeftLateralCountWildcard() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 30, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            a	ts	cnt
                            1	2024-01-01T00:00:00.000000Z	2
                            2	2024-01-01T01:00:00.000000Z	1
                            3	2024-01-01T02:00:00.000000Z	0
                            """,
                    """
                            SELECT *
                            FROM t1
                            LEFT JOIN LATERAL (
                                SELECT count(*) AS cnt FROM t2 WHERE t2.t1_id = t1.a
                            ) t ON true
                            ORDER BY t1.a
                            """,
                    null, true, false
            );
        });
    }

    // T56b: SELECT * + LEFT LATERAL + SAMPLE BY + count(*) — count must be 0, no __qdb_outer_ref__ columns
    @Test
    public void testT56bLeftLateralSampleByCountWildcard() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:40:00.000000Z'),
                    (2, 30, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            a\tts\tts1\tcnt\ttotal
                            1\t2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:00.000000Z\t1\t10
                            1\t2024-01-01T00:00:00.000000Z\t2024-01-01T00:30:00.000000Z\t1\t20
                            2\t2024-01-01T01:00:00.000000Z\t2024-01-01T01:00:00.000000Z\t1\t30
                            3\t2024-01-01T02:00:00.000000Z\t\t0\tnull
                            """,
                    """
                            SELECT *
                            FROM t1
                            LEFT JOIN LATERAL (
                                SELECT ts, count(*) AS cnt, sum(val) AS total
                                FROM t2
                                WHERE t2.t1_id = t1.a
                                SAMPLE BY 30m
                            ) t ON true
                            ORDER BY t1.a, t.ts
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT57LeftJoinInLateralPreservesNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE items (id INT, order_id INT, name STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE tags (item_id INT, order_id INT, tag STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO items VALUES
                    (10, 1, 'Widget', '2024-01-01T00:10:00.000000Z'),
                    (20, 2, 'Gadget', '2024-01-01T01:10:00.000000Z')
                    """);
            // Only order 1's item has a tag — order 2's item should get NULL fill
            execute("""
                    INSERT INTO tags VALUES
                    (10, 1, 'premium', '2024-01-01T00:20:00.000000Z')
                    """);

            // LEFT JOIN inside LATERAL: tags.order_id = o.id is correlated and in ON clause.
            // It must stay there so that items without matching tags get NULL fill.
            assertQueryNoLeakCheck(
                    """
                            id\tname\ttag
                            1\tWidget\tpremium
                            2\tGadget\t
                            """,
                    """
                            SELECT o.id, sub.name, sub.tag
                            FROM orders o
                            JOIN LATERAL (
                                SELECT i.name, t.tag
                                FROM items i
                                LEFT JOIN tags t ON t.item_id = i.id AND t.order_id = o.id
                                WHERE i.order_id = o.id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    // T58: LIMIT with function ORDER BY — compensateLimit must find LIMIT and ORDER BY
    // on different wrapper layers and resolve ORDER BY aliases through intermediate SELECTs
    @Test
    public void testT58LimitWithFunctionOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 3, '2024-01-01T00:10:00.000000Z'),
                    (1, 7, '2024-01-01T00:20:00.000000Z'),
                    (1, 1, '2024-01-01T00:30:00.000000Z'),
                    (2, 10, '2024-01-01T01:10:00.000000Z'),
                    (2, 4, '2024-01-01T01:20:00.000000Z'),
                    (2, 8, '2024-01-01T01:30:00.000000Z')
                    """);

            // ORDER BY abs(val - 5) triggers moveOrderByFunctionsIntoOuterSelect.
            // Per-group LIMIT 2: for each t1 row, take 2 vals closest to 5.
            assertQueryNoLeakCheck(
                    """
                            a\tval
                            1\t3
                            1\t7
                            2\t4
                            2\t8
                            """,
                    """
                            SELECT t1.a, sub.val
                            FROM t1
                            JOIN LATERAL (
                                SELECT val FROM t2
                                WHERE t2.t1_id = t1.a
                                ORDER BY abs(val - 5)
                                LIMIT 2
                            ) sub
                            ORDER BY t1.a, sub.val
                            """,
                    null, true, false
            );
        });
    }

    // T59: LEFT LATERAL + LIMIT — per-group LIMIT with NULL fill for no-match groups
    @Test
    public void testT59LeftLateralLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (1, 30, '2024-01-01T00:30:00.000000Z'),
                    (2, 40, '2024-01-01T01:10:00.000000Z')
                    """);

            // t1.a=3 has no matching t2 rows → NULL fill from LEFT JOIN
            assertQueryNoLeakCheck(
                    """
                            a\tval
                            1\t30
                            1\t20
                            2\t40
                            3\tnull
                            """,
                    """
                            SELECT t1.a, sub.val
                            FROM t1
                            LEFT JOIN LATERAL (
                                SELECT val FROM t2
                                WHERE t2.t1_id = t1.a
                                ORDER BY val DESC
                                LIMIT 2
                            ) sub
                            ORDER BY t1.a, sub.val DESC
                            """,
                    null, true, false
            );
        });
    }

    // T60: LIMIT with OFFSET — per-group LIMIT offset,count
    @Test
    public void testT60LimitWithOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (1, 30, '2024-01-01T00:30:00.000000Z'),
                    (1, 40, '2024-01-01T00:40:00.000000Z'),
                    (2, 50, '2024-01-01T01:10:00.000000Z'),
                    (2, 60, '2024-01-01T01:20:00.000000Z'),
                    (2, 70, '2024-01-01T01:30:00.000000Z'),
                    (2, 80, '2024-01-01T01:40:00.000000Z')
                    """);

            // LIMIT 1, 2: rows in [1, 2) — skip 1 row, take 1 row per group (ORDER BY val DESC)
            assertQueryNoLeakCheck(
                    """
                            a\tval
                            1\t30
                            2\t70
                            """,
                    """
                            SELECT t1.a, sub.val
                            FROM t1
                            JOIN LATERAL (
                                SELECT val FROM t2
                                WHERE t2.t1_id = t1.a
                                ORDER BY val DESC
                                LIMIT 1, 2
                            ) sub
                            ORDER BY t1.a, sub.val DESC
                            """,
                    null, true, false
            );
        });
    }

    // T61: Multiple lateral joins on the same outer table — verifies outerRefId uniqueness
    @Test
    public void testT61MultipleLateralJoinsOnSameOuter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE fills (id INT, order_id INT, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO fills VALUES
                    (1, 1, 100.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 200.0, '2024-01-01T01:10:00.000000Z'),
                    (3, 2, 300.0, '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\ttrade_cnt\tfill_cnt
                            1\t2\t1
                            2\t1\t2
                            """,
                    """
                            SELECT o.id, t.trade_cnt, f.fill_cnt
                            FROM orders o
                            JOIN LATERAL (
                                SELECT count(*) AS trade_cnt FROM trades WHERE order_id = o.id
                            ) t
                            JOIN LATERAL (
                                SELECT count(*) AS fill_cnt FROM fills WHERE order_id = o.id
                            ) f
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT62CorrelationFromTwoOuterTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE customers (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE products (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE sales (customer_id INT, product_id INT, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO customers VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO products VALUES
                    (10, '2024-01-01T00:00:00.000000Z'),
                    (20, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO sales VALUES
                    (1, 10, 5, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, 3, '2024-01-01T00:20:00.000000Z'),
                    (2, 10, 7, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tid1\ttotal_qty
                            1\t10\t5
                            1\t20\t3
                            2\t10\t7
                            2\t20\tnull
                            """,
                    """
                            SELECT c.id, p.id, s.total_qty
                            FROM customers c
                            CROSS JOIN products p
                            LEFT JOIN LATERAL (
                                SELECT sum(qty)::INT AS total_qty
                                FROM sales
                                WHERE customer_id = c.id AND product_id = p.id
                            ) s
                            ORDER BY c.id, p.id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT62bLateralOnNonEqualityFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, n INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 2, '2024-01-01T00:00:00.000000Z'),
                    (2, 3, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (1, 30, '2024-01-01T00:30:00.000000Z'),
                    (2, 40, '2024-01-01T01:10:00.000000Z'),
                    (2, 50, '2024-01-01T01:20:00.000000Z'),
                    (2, 60, '2024-01-01T01:30:00.000000Z'),
                    (2, 70, '2024-01-01T01:40:00.000000Z')
                    """);

            // t1.n controls how many rows from t2 to keep via ON rn <= t1.n
            // id=1, n=2 → keep rows with rn<=2 → val 10, 20
            // id=2, n=3 → keep rows with rn<=3 → val 40, 50, 60
            assertQueryNoLeakCheck(
                    """
                            id\tval\trn
                            1\t10\t1
                            1\t20\t2
                            2\t40\t1
                            2\t50\t2
                            2\t60\t3
                            """,
                    """
                            SELECT t1.id, sub.val, sub.rn
                            FROM t1
                            JOIN LATERAL (
                                SELECT val, row_number() OVER (ORDER BY ts) AS rn
                                FROM t2
                                WHERE t2.t1_id = t1.id
                            ) sub ON sub.rn <= t1.n
                            ORDER BY t1.id, sub.rn
                            """,
                    null, true, false
            );
        });
    }

    // T62c2: LEFT LATERAL with ON non-equality condition — NULL fill for unmatched + ON filter
    @Test
    public void testT62c2LeftLateralOnFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, n INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 2, '2024-01-01T00:00:00.000000Z'),
                    (2, 1, '2024-01-01T01:00:00.000000Z'),
                    (3, 5, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (1, 30, '2024-01-01T00:30:00.000000Z'),
                    (2, 40, '2024-01-01T01:10:00.000000Z'),
                    (2, 50, '2024-01-01T01:20:00.000000Z')
                    """);

            // LEFT + ON rn <= t1.n: unmatched rows get NULL
            // id=1, n=2: rows with rn<=2 → val 10,20
            // id=2, n=1: rows with rn<=1 → val 40
            // id=3, n=5: no trades for t1_id=3 → NULL
            assertQueryNoLeakCheck(
                    """
                            id\tval\trn
                            1\t10\t1
                            1\t20\t2
                            2\t40\t1
                            3\tnull\tnull
                            """,
                    """
                            SELECT t1.id, sub.val, sub.rn
                            FROM t1
                            LEFT JOIN LATERAL (
                                SELECT val, row_number() OVER (ORDER BY ts) AS rn
                                FROM t2
                                WHERE t2.t1_id = t1.id
                            ) sub ON sub.rn <= t1.n
                            ORDER BY t1.id, sub.rn
                            """,
                    null, true, false
            );
        });
    }

    // T62c: LATERAL ON with equality + non-equality conditions mixed
    @Test
    public void testT62cLateralOnMixedConditions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, cat SYMBOL, n INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, cat SYMBOL, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 'A', 2, '2024-01-01T00:00:00.000000Z'),
                    (2, 'B', 3, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 'A', 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 'A', 20, '2024-01-01T00:20:00.000000Z'),
                    (1, 'A', 30, '2024-01-01T00:30:00.000000Z'),
                    (1, 'X', 99, '2024-01-01T00:40:00.000000Z'),
                    (2, 'B', 40, '2024-01-01T01:10:00.000000Z'),
                    (2, 'B', 50, '2024-01-01T01:20:00.000000Z'),
                    (2, 'B', 60, '2024-01-01T01:30:00.000000Z'),
                    (2, 'B', 70, '2024-01-01T01:40:00.000000Z')
                    """);

            // ON has equality (sub.cat = t1.cat) + non-equality (sub.rn <= t1.n)
            // id=1, cat=A, n=2: cat filter excludes val=99(cat=X), rn filter keeps val 10,20
            // id=2, cat=B, n=3: rn filter keeps val 40,50,60
            assertQueryNoLeakCheck(
                    """
                            id\tval\trn
                            1\t10\t1
                            1\t20\t2
                            2\t40\t1
                            2\t50\t2
                            2\t60\t3
                            """,
                    """
                            SELECT t1.id, sub.val, sub.rn
                            FROM t1
                            JOIN LATERAL (
                                SELECT val, cat, row_number() OVER (ORDER BY ts) AS rn
                                FROM t2
                                WHERE t2.t1_id = t1.id
                            ) sub ON sub.cat = t1.cat AND sub.rn <= t1.n
                            ORDER BY t1.id, sub.rn
                            """,
                    null, true, false
            );
        });
    }

    // T62d: LATERAL ON with pure equality condition (separate from WHERE correlation)
    @Test
    public void testT62dLateralOnEqualityFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, cat SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, cat SYMBOL, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 'A', '2024-01-01T00:00:00.000000Z'),
                    (2, 'B', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 'A', 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 'X', 99, '2024-01-01T00:20:00.000000Z'),
                    (1, 'A', 20, '2024-01-01T00:30:00.000000Z'),
                    (2, 'B', 30, '2024-01-01T01:10:00.000000Z'),
                    (2, 'Z', 88, '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\t10
                            1\t20
                            2\t30
                            """,
                    """
                            SELECT t1.id, sub.val
                            FROM t1
                            JOIN LATERAL (
                                SELECT val, cat
                                FROM t2
                                WHERE t2.t1_id = t1.id
                            ) sub ON sub.cat = t1.cat
                            ORDER BY t1.id, sub.val
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT63MmComplianceCheck() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE mm_book (
                        mm_id SYMBOL, symbol SYMBOL, side SYMBOL,
                        price DOUBLE, qty DOUBLE,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    CREATE TABLE mm_obligations (
                        mm_id SYMBOL, symbol SYMBOL, obligation_id INT,
                        min_qty DOUBLE, max_spread DOUBLE, spread_type SYMBOL,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            execute("""
                    INSERT INTO mm_book VALUES
                    ('mm1', 'AAPL', 'BID', 100.0, 100.0, '2024-01-01T00:00:00.000000Z'),
                    ('mm1', 'AAPL', 'BID',  99.0, 100.0, '2024-01-01T00:00:01.000000Z'),
                    ('mm1', 'AAPL', 'BID',  98.0, 100.0, '2024-01-01T00:00:02.000000Z'),
                    ('mm1', 'AAPL', 'ASK', 102.0, 100.0, '2024-01-01T00:00:03.000000Z'),
                    ('mm1', 'AAPL', 'ASK', 103.0, 100.0, '2024-01-01T00:00:04.000000Z'),
                    ('mm1', 'AAPL', 'ASK', 104.0, 100.0, '2024-01-01T00:00:05.000000Z'),
                    ('mm2', 'AAPL', 'BID',  50.0, 100.0, '2024-01-01T00:00:06.000000Z'),
                    ('mm3', 'MSFT', 'BID', 100.0, 200.0, '2024-01-01T00:00:07.000000Z'),
                    ('mm3', 'MSFT', 'ASK', 125.0, 200.0, '2024-01-01T00:00:08.000000Z'),
                    ('mm4', 'MSFT', 'BID',  50.0, 200.0, '2024-01-01T00:00:09.000000Z'),
                    ('mm4', 'MSFT', 'ASK',  51.0, 200.0, '2024-01-01T00:00:10.000000Z'),
                    ('mm5', 'GOOG', 'BID', 100.0,  30.0, '2024-01-01T00:00:11.000000Z'),
                    ('mm5', 'GOOG', 'ASK', 101.0,  30.0, '2024-01-01T00:00:12.000000Z'),
                    ('mm6', 'GOOG', 'ASK', 100.0, 200.0, '2024-01-01T00:00:13.000000Z')
                    """);
            execute("""
                    INSERT INTO mm_obligations VALUES
                    ('mm1', 'AAPL', 1, 100.0, 0.05,  'Pct', '2024-01-01T00:00:00.000000Z'),
                    ('mm2', 'AAPL', 2, 100.0, 0.05,  'Pct', '2024-01-01T00:00:01.000000Z'),
                    ('mm3', 'MSFT', 3, 100.0, 0.05,  'Pct', '2024-01-01T00:00:02.000000Z'),
                    ('mm4', 'MSFT', 4, 100.0, 2.0,   'Abs', '2024-01-01T00:00:03.000000Z'),
                    ('mm5', 'GOOG', 5,  50.0, 0.05,  'Pct', '2024-01-01T00:00:04.000000Z'),
                    ('mm6', 'GOOG', 6, 100.0, 0.05,  'Pct', '2024-01-01T00:00:05.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            mm_id\tsymbol\tobligation_id\tnon_compliance_reason\tavg_price_buy\tavg_price_sell\tqty_buy\tqty_sell\tspread
                            mm1\tAAPL\t1\tNone\t100.0\t102.0\t300.0\t300.0\t0.020000000000000018
                            mm2\tAAPL\t2\tSellQuantity\tnull\tnull\t100.0\t0.0\tnull
                            mm3\tMSFT\t3\tSpread\t100.0\t125.0\t200.0\t200.0\t0.25
                            mm4\tMSFT\t4\tNone\t50.0\t51.0\t200.0\t200.0\t1.0
                            mm5\tGOOG\t5\tQuantity\tnull\tnull\t30.0\t30.0\tnull
                            mm6\tGOOG\t6\tBuyQuantity\tnull\tnull\t0.0\t200.0\tnull
                            """,
                    """
                             WITH
                              side_totals AS (
                                SELECT mm_id, symbol, side, sum(qty) AS total_qty
                                FROM mm_book
                                WHERE qty > 0
                                GROUP BY mm_id, symbol, side
                              ),
                              both_sides AS (
                                SELECT mm_id, symbol,
                                       coalesce(sum(CASE WHEN side = 'BID' THEN total_qty END), 0) AS qty_buy,
                                       coalesce(sum(CASE WHEN side = 'ASK' THEN total_qty END), 0) AS qty_sell
                                FROM side_totals
                                GROUP BY mm_id, symbol
                              ),
                              with_obligations AS (
                                SELECT b.mm_id, b.symbol, b.qty_buy, b.qty_sell,
                                       o.obligation_id, o.min_qty, o.max_spread, o.spread_type
                                FROM both_sides b
                                JOIN mm_obligations o ON b.mm_id = o.mm_id AND b.symbol = o.symbol
                              ),
                              pre_check AS (
                                SELECT *,
                                       CASE
                                         WHEN qty_buy = 0 AND qty_sell = 0 THEN 'NoOrders'
                                         WHEN qty_buy = 0               THEN 'BuyQuantity'
                                         WHEN qty_sell = 0              THEN 'SellQuantity'
                                         WHEN qty_buy < min_qty AND qty_sell < min_qty THEN 'Quantity'
                                         WHEN qty_buy < min_qty         THEN 'BuyQuantity'
                                         WHEN qty_sell < min_qty        THEN 'SellQuantity'
                                         ELSE NULL
                                       END AS early_fail
                                FROM with_obligations
                              ),
                              bid_sweep AS (
                                SELECT p.mm_id, p.symbol, p.obligation_id, p.min_qty,
                                       sum(bk.price * LEAST(bk.qty,
                                           GREATEST(0, p.min_qty - (bk.cum_qty - bk.qty)))
                                       ) / p.min_qty AS avg_price_buy
                                FROM pre_check p
                                JOIN LATERAL (
                                  SELECT price, qty,
                                         sum(qty) OVER (ORDER BY price DESC) AS cum_qty
                                  FROM mm_book
                                  WHERE mm_id = p.mm_id AND symbol = p.symbol
                                    AND side = 'BID' AND qty > 0
                                ) bk ON bk.cum_qty - bk.qty < p.min_qty
                                WHERE p.early_fail IS NULL
                                GROUP BY p.mm_id, p.symbol, p.obligation_id, p.min_qty
                              ),
                              ask_sweep AS (
                                SELECT p.mm_id, p.symbol, p.obligation_id, p.min_qty,
                                       sum(bk.price * LEAST(bk.qty,
                                           GREATEST(0, p.min_qty - (bk.cum_qty - bk.qty)))
                                       ) / p.min_qty AS avg_price_sell
                                FROM pre_check p
                                JOIN LATERAL (
                                  SELECT price, qty,
                                         sum(qty) OVER (ORDER BY price ASC) AS cum_qty
                                  FROM mm_book
                                  WHERE mm_id = p.mm_id AND symbol = p.symbol
                                    AND side = 'ASK' AND qty > 0
                                ) bk ON bk.cum_qty - bk.qty < p.min_qty
                                WHERE p.early_fail IS NULL
                                GROUP BY p.mm_id, p.symbol, p.obligation_id, p.min_qty
                              )
                            SELECT
                              p.mm_id, p.symbol, p.obligation_id,
                              CASE
                                WHEN p.early_fail IS NOT NULL THEN p.early_fail
                                WHEN p.spread_type = 'Pct'
                                  AND (a.avg_price_sell / b.avg_price_buy) - 1 > p.max_spread THEN 'Spread'
                                WHEN p.spread_type = 'Abs'
                                  AND a.avg_price_sell - b.avg_price_buy > p.max_spread THEN 'Spread'
                                ELSE 'None'
                              END AS non_compliance_reason,
                              b.avg_price_buy,
                              a.avg_price_sell,
                              p.qty_buy,
                              p.qty_sell,
                              CASE WHEN p.spread_type = 'Pct'
                                THEN (a.avg_price_sell / b.avg_price_buy) - 1
                                ELSE a.avg_price_sell - b.avg_price_buy
                              END AS spread
                            FROM pre_check p
                            LEFT JOIN bid_sweep b ON p.mm_id = b.mm_id AND p.symbol = b.symbol
                              AND p.obligation_id = b.obligation_id
                            LEFT JOIN ask_sweep a ON p.mm_id = a.mm_id AND p.symbol = a.symbol
                              AND p.obligation_id = a.obligation_id
                            ORDER BY p.mm_id, p.symbol
                            """,
                    null, true, false
            );
        });
    }

    // TODO: mat views do not support window functions on the base table
    @Ignore
    @Test
    public void testT63bMmComplianceMatView() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE mm_book (
                        mm_id SYMBOL, symbol SYMBOL, side SYMBOL,
                        price DOUBLE, qty DOUBLE,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    CREATE TABLE mm_obligations (
                        mm_id SYMBOL, symbol SYMBOL, obligation_id INT,
                        min_qty DOUBLE, max_spread DOUBLE, spread_type SYMBOL,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            execute("""
                    CREATE MATERIALIZED VIEW mm_compliance WITH BASE mm_book AS (
                    WITH
                      side_totals AS (
                        SELECT mm_id, symbol, side, sum(qty) AS total_qty
                        FROM mm_book
                        WHERE qty > 0
                        GROUP BY mm_id, symbol, side
                      ),
                      both_sides AS (
                        SELECT mm_id, symbol,
                               coalesce(sum(CASE WHEN side = 'BID' THEN total_qty END), 0) AS qty_buy,
                               coalesce(sum(CASE WHEN side = 'ASK' THEN total_qty END), 0) AS qty_sell
                        FROM side_totals
                        GROUP BY mm_id, symbol
                      ),
                      with_obligations AS (
                        SELECT b.mm_id, b.symbol, b.qty_buy, b.qty_sell,
                               o.obligation_id, o.min_qty, o.max_spread, o.spread_type
                        FROM both_sides b
                        JOIN mm_obligations o ON b.mm_id = o.mm_id AND b.symbol = o.symbol
                      ),
                      pre_check AS (
                        SELECT *,
                               CASE
                                 WHEN qty_buy = 0 AND qty_sell = 0 THEN 'NoOrders'
                                 WHEN qty_buy = 0               THEN 'BuyQuantity'
                                 WHEN qty_sell = 0              THEN 'SellQuantity'
                                 WHEN qty_buy < min_qty AND qty_sell < min_qty THEN 'Quantity'
                                 WHEN qty_buy < min_qty         THEN 'BuyQuantity'
                                 WHEN qty_sell < min_qty        THEN 'SellQuantity'
                                 ELSE NULL
                               END AS early_fail
                        FROM with_obligations
                      ),
                      bid_sweep AS (
                        SELECT p.mm_id, p.symbol, p.obligation_id, p.min_qty,
                               sum(bk.price * LEAST(bk.qty,
                                   GREATEST(0, p.min_qty - (bk.cum_qty - bk.qty)))
                               ) / p.min_qty AS avg_price_buy
                        FROM pre_check p
                        JOIN LATERAL (
                          SELECT price, qty,
                                 sum(qty) OVER (ORDER BY price DESC) AS cum_qty
                          FROM mm_book
                          WHERE mm_id = p.mm_id AND symbol = p.symbol
                            AND side = 'BID' AND qty > 0
                        ) bk ON bk.cum_qty - bk.qty < p.min_qty
                        WHERE p.early_fail IS NULL
                        GROUP BY p.mm_id, p.symbol, p.obligation_id, p.min_qty
                      ),
                      ask_sweep AS (
                        SELECT p.mm_id, p.symbol, p.obligation_id, p.min_qty,
                               sum(bk.price * LEAST(bk.qty,
                                   GREATEST(0, p.min_qty - (bk.cum_qty - bk.qty)))
                               ) / p.min_qty AS avg_price_sell
                        FROM pre_check p
                        JOIN LATERAL (
                          SELECT price, qty,
                                 sum(qty) OVER (ORDER BY price ASC) AS cum_qty
                          FROM mm_book
                          WHERE mm_id = p.mm_id AND symbol = p.symbol
                            AND side = 'ASK' AND qty > 0
                        ) bk ON bk.cum_qty - bk.qty < p.min_qty
                        WHERE p.early_fail IS NULL
                        GROUP BY p.mm_id, p.symbol, p.obligation_id, p.min_qty
                      )
                    SELECT
                      p.mm_id, p.symbol, p.obligation_id,
                      CASE
                        WHEN p.early_fail IS NOT NULL THEN p.early_fail
                        WHEN p.spread_type = 'Pct'
                          AND (a.avg_price_sell / b.avg_price_buy) - 1 > p.max_spread THEN 'Spread'
                        WHEN p.spread_type = 'Abs'
                          AND a.avg_price_sell - b.avg_price_buy > p.max_spread THEN 'Spread'
                        ELSE 'None'
                      END AS non_compliance_reason,
                      b.avg_price_buy,
                      a.avg_price_sell,
                      p.qty_buy,
                      p.qty_sell,
                      CASE WHEN p.spread_type = 'Pct'
                        THEN (a.avg_price_sell / b.avg_price_buy) - 1
                        ELSE a.avg_price_sell - b.avg_price_buy
                      END AS spread
                    FROM pre_check p
                    LEFT JOIN bid_sweep b ON p.mm_id = b.mm_id AND p.symbol = b.symbol
                      AND p.obligation_id = b.obligation_id
                    LEFT JOIN ask_sweep a ON p.mm_id = a.mm_id AND p.symbol = a.symbol
                      AND p.obligation_id = a.obligation_id
                    ORDER BY p.mm_id, p.symbol
                    )
                    """);

            execute("""
                    INSERT INTO mm_book VALUES
                    ('mm1', 'AAPL', 'BID', 100.0, 200.0, '2024-01-01T00:00:00.000000Z'),
                    ('mm1', 'AAPL', 'ASK', 102.0, 200.0, '2024-01-01T00:00:01.000000Z'),
                    ('mm2', 'AAPL', 'BID',  50.0, 100.0, '2024-01-01T00:00:02.000000Z')
                    """);
            execute("""
                    INSERT INTO mm_obligations VALUES
                    ('mm1', 'AAPL', 1, 100.0, 0.05, 'Pct', '2024-01-01T00:00:00.000000Z'),
                    ('mm2', 'AAPL', 2, 100.0, 0.05, 'Pct', '2024-01-01T00:00:01.000000Z')
                    """);
            drainWalAndMatViewQueues();

            assertQueryNoLeakCheck(
                    """
                            mm_id\tsymbol\tobligation_id\tnon_compliance_reason\tavg_price_buy\tavg_price_sell\tqty_buy\tqty_sell\tspread
                            mm1\tAAPL\t1\tNone\t100.0\t102.0\t200.0\t200.0\t0.020000000000000018
                            mm2\tAAPL\t2\tSellQuantity\tnull\tnull\t100.0\t0.0\tnull
                            """,
                    "SELECT * FROM mm_compliance ORDER BY mm_id, symbol",
                    null, true, false
            );
        });
    }

    // T64: Cascading lateral — lateral B references lateral A's output
    @Test
    public void testT64CascadingLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE fees (order_id INT, qty_threshold DOUBLE, fee DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO fees VALUES
                    (1, 15.0, 0.5, '2024-01-01T00:00:00.000000Z'),
                    (1, 25.0, 1.0, '2024-01-01T00:00:01.000000Z'),
                    (2, 10.0, 0.3, '2024-01-01T01:00:00.000000Z'),
                    (2, 50.0, 2.0, '2024-01-01T01:00:01.000000Z')
                    """);

            // lateral A: total_qty per order
            // lateral B: fees where qty_threshold < A.total_qty (references A's output)
            // order 1: total_qty=30 → fees with threshold < 30: 15(0.5), 25(1.0)
            // order 2: total_qty=30 → fees with threshold < 30: 10(0.3)
            // order 3: no trades → dropped by INNER
            assertQueryNoLeakCheck(
                    """
                            id\ttotal_qty\tfee
                            1\t30.0\t0.5
                            1\t30.0\t1.0
                            2\t30.0\t0.3
                            """,
                    """
                            SELECT o.id, a.total_qty, b.fee
                            FROM orders o
                            JOIN LATERAL (
                                SELECT sum(qty) AS total_qty FROM trades WHERE order_id = o.id
                            ) a
                            JOIN LATERAL (
                                SELECT fee FROM fees
                                WHERE order_id = o.id AND qty_threshold < a.total_qty
                            ) b
                            ORDER BY o.id, b.fee
                            """,
                    null, true, false
            );
        });
    }

    // T64b: Cascading lateral where first lateral has window function — verifies deepClone
    // handles WindowExpression correctly. Without deep clone, the shared model between
    // orders.jm[1] and the second lateral's outer ref subquery causes rewriteSelectClause0
    // to corrupt the model on the second processing pass.
    @Test
    public void testT64bCascadingLateralWithWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE bonuses (min_rank LONG, bonus DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (1, 30.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 40.0, '2024-01-01T01:10:00.000000Z'),
                    (2, 50.0, '2024-01-01T01:20:00.000000Z')
                    """);
            execute("""
                    INSERT INTO bonuses VALUES
                    (1, 100.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 200.0, '2024-01-01T00:00:01.000000Z'),
                    (3, 300.0, '2024-01-01T00:00:02.000000Z')
                    """);

            // Lateral A: window function (row_number) per order — top trade by qty
            // Lateral B: references A's rank to find matching bonuses
            // order 1: top trade qty=30 rank=1 → bonuses with min_rank <= 1: bonus=100
            // order 2: top trade qty=50 rank=1 → bonuses with min_rank <= 1: bonus=100
            assertQueryNoLeakCheck(
                    """
                            id\tqty\trank\tbonus
                            1\t30.0\t1\t100.0
                            2\t50.0\t1\t100.0
                            """,
                    """
                            SELECT o.id, a.qty, a.rank, b.bonus
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty, rank FROM (
                                    SELECT qty, row_number() OVER (ORDER BY qty DESC) AS rank
                                    FROM trades
                                    WHERE order_id = o.id
                                ) WHERE rank = 1
                            ) a
                            JOIN LATERAL (
                                SELECT bonus FROM bonuses
                                WHERE min_rank <= a.rank
                                ORDER BY bonus DESC
                                LIMIT 1
                            ) b
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T65: 3-level nested lateral — t1 → lateral(t2 → lateral(t3 → lateral(t4)))
    @Test
    public void testT65ThreeLevelNestedLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (b INT, t1_a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t3 (c INT, t2_b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t4 (d INT, t3_c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t1 VALUES (1, '2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO t2 VALUES (10, 1, '2024-01-01T00:10:00.000000Z')");
            execute("INSERT INTO t3 VALUES (100, 10, '2024-01-01T00:20:00.000000Z')");
            execute("INSERT INTO t4 VALUES (1000, 100, '2024-01-01T00:30:00.000000Z')");

            assertQueryNoLeakCheck(
                    """
                            a\tb\tc\td
                            1\t10\t100\t1000
                            """,
                    """
                            SELECT t1.a, s1.b, s1.c, s1.d
                            FROM t1
                            JOIN LATERAL (
                                SELECT t2.b, s2.c, s2.d
                                FROM t2
                                JOIN LATERAL (
                                    SELECT t3.c, s3.d
                                    FROM t3
                                    JOIN LATERAL (
                                        SELECT d FROM t4 WHERE t4.t3_c = t3.c
                                    ) s3
                                    WHERE t3.t2_b = t2.b
                                ) s2
                                WHERE t2.t1_a = t1.a
                            ) s1
                            """,
                    null, false, true
            );
        });
    }

    // T66: 3-way UNION ALL inside lateral
    @Test
    public void testT66ThreeWayUnionAll() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (order_id INT, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (order_id INT, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_c (order_id INT, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("INSERT INTO trades_a VALUES (1, 10, '2024-01-01T00:10:00.000000Z')");
            execute("INSERT INTO trades_b VALUES (1, 20, '2024-01-01T00:20:00.000000Z'), (2, 30, '2024-01-01T01:10:00.000000Z')");
            execute("INSERT INTO trades_c VALUES (2, 40, '2024-01-01T01:20:00.000000Z')");

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10
                            1\t20
                            2\t30
                            2\t40
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                UNION ALL
                                SELECT qty FROM trades_b WHERE order_id = o.id
                                UNION ALL
                                SELECT qty FROM trades_c WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, true
            );
        });
    }

    // T67: UNION ALL with heterogeneous branches (aggregate vs scan)
    @Test
    public void testT67UnionHeterogeneousBranches() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // Branch 1: aggregate (sum), Branch 2: scan (individual rows)
            // order 1: sum=30 + rows 10,20 → 10,20,30
            // order 2: sum=30 + row 30 → 30,30 (duplicate value from two branches)
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\t10.0
                            1\t20.0
                            1\t30.0
                            2\t30.0
                            2\t30.0
                            """,
                    """
                            SELECT o.id, t.val
                            FROM orders o
                            JOIN LATERAL (
                                SELECT sum(qty) AS val FROM trades WHERE order_id = o.id
                                UNION ALL
                                SELECT qty AS val FROM trades WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.val
                            """,
                    null, true, true
            );
        });
    }

    // T68: Nested lateral where inner references outermost level (skip level)
    @Test
    public void testT68NestedLateralSkipLevel() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (b INT, t1_a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t3 (c INT, t1_a INT, t2_b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t1 VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO t2 VALUES
                    (10, 1, '2024-01-01T00:10:00.000000Z'),
                    (20, 2, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t3 VALUES
                    (100, 1, 10, '2024-01-01T00:20:00.000000Z'),
                    (200, 2, 20, '2024-01-01T01:20:00.000000Z'),
                    (300, 1, 20, '2024-01-01T01:30:00.000000Z')
                    """);

            // Inner lateral references both t2 (immediate parent) and t1 (grandparent)
            // t1.a=1, t2.b=10: t3 where t1_a=1 AND t2_b=10 → c=100
            // t1.a=2, t2.b=20: t3 where t1_a=2 AND t2_b=20 → c=200
            assertQueryNoLeakCheck(
                    """
                            a\tb\tc
                            1\t10\t100
                            2\t20\t200
                            """,
                    """
                            SELECT t1.a, s1.b, s1.c
                            FROM t1
                            JOIN LATERAL (
                                SELECT t2.b, s2.c
                                FROM t2
                                JOIN LATERAL (
                                    SELECT c FROM t3
                                    WHERE t3.t1_a = t1.a AND t3.t2_b = t2.b
                                ) s2
                                WHERE t2.t1_a = t1.a
                            ) s1
                            ORDER BY t1.a
                            """,
                    null, true, true
            );
        });
    }

    // T69: Cascading lateral with LEFT — second lateral references first, unmatched rows
    @Test
    public void testT69CascadingLateralLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE discounts (min_qty DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 100.0, '2024-01-01T00:10:00.000000Z'),
                    (2,  20.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO discounts VALUES
                    (50.0, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (80.0, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            // A: total_qty per order (LEFT → order 3 gets null)
            // B: discounts where min_qty <= A.total_qty (LEFT → order 2 qty=20 gets no discount)
            // order 1: qty=100 → discounts 50(0.1), 80(0.2)
            // order 2: qty=20 → no discount match → null
            // order 3: no trades → null total_qty → null
            assertQueryNoLeakCheck(
                    """
                            id\ttotal_qty\trate
                            1\t100.0\t0.1
                            1\t100.0\t0.2
                            2\t20.0\tnull
                            3\tnull\tnull
                            """,
                    """
                            SELECT o.id, a.total_qty, b.rate
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT sum(qty) AS total_qty FROM trades WHERE order_id = o.id
                            ) a
                            LEFT JOIN LATERAL (
                                SELECT rate FROM discounts WHERE min_qty <= a.total_qty
                            ) b
                            ORDER BY o.id, b.rate
                            """,
                    null, true, false
            );
        });
    }

    // T70: Multiple correlated joins inside lateral — both inner join branches reference outer
    @Test
    public void testT70MultipleCorrelatedJoinsInsideLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE items (order_id INT, product STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE payments (order_id INT, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO items VALUES
                    (1, 'Apple', '2024-01-01T00:10:00.000000Z'),
                    (1, 'Banana', '2024-01-01T00:20:00.000000Z'),
                    (2, 'Cherry', '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO payments VALUES
                    (1, 50.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z'),
                    (2, 20.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Inside the lateral: CROSS JOIN of two subqueries, BOTH correlated to outer o.id
            assertQueryNoLeakCheck(
                    """
                            id\titem_cnt\ttotal_paid
                            1\t2\t50.0
                            2\t1\t50.0
                            """,
                    """
                            SELECT o.id, sub.item_cnt, sub.total_paid
                            FROM orders o
                            JOIN LATERAL (
                                SELECT ic.item_cnt, ps.total_paid
                                FROM (SELECT count(*)::INT AS item_cnt FROM items WHERE order_id = o.id) ic
                                CROSS JOIN (SELECT sum(amount) AS total_paid FROM payments WHERE order_id = o.id) ps
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    // T71: Post-join WHERE filter on lateral output + aggregate filter inside lateral
    @Test
    public void testT71PostJoinFilterAndAggregateFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 'X', 5.0,  '2024-01-01T00:10:00.000000Z'),
                    (1, 'X', 15.0, '2024-01-01T00:20:00.000000Z'),
                    (1, 'Y', 3.0,  '2024-01-01T00:30:00.000000Z'),
                    (2, 'X', 25.0, '2024-01-01T01:10:00.000000Z'),
                    (2, 'Y', 30.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Inner subquery: GROUP BY + WHERE total > 10 filters small groups
            // Post-join WHERE t.total > 20 further filters results
            // order 1: cat X total=20 (passes inner, fails post-join), cat Y total=3 (fails inner)
            // order 2: cat X total=25 (passes both), cat Y total=30 (passes both)
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\ttotal
                            2\tX\t25.0
                            2\tY\t30.0
                            """,
                    """
                            SELECT o.id, t.category, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, total FROM (
                                    SELECT category, sum(qty) AS total
                                    FROM trades
                                    WHERE order_id = o.id
                                    GROUP BY category
                                ) WHERE total > 10
                            ) t
                            WHERE t.total > 20
                            ORDER BY o.id, t.category
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT72FunctionLimitExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (1, 30, '2024-01-01T00:30:00.000000Z')
                    """);

            // Non-constant LIMIT expression: abs(2) = 2
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\t10
                            1\t20
                            """,
                    """
                            SELECT t1.id, sub.val
                            FROM t1
                            JOIN LATERAL (
                                SELECT val FROM t2 WHERE t2.t1_id = t1.id ORDER BY val LIMIT abs(2)
                            ) sub
                            ORDER BY t1.id, sub.val
                            """,
                    null, true, false
            );
        });
    }

    // T73: Window function embedded in expression (not top-level WindowExpression column)
    @Test
    public void testT73EmbeddedWindowExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 30, '2024-01-01T01:10:00.000000Z'),
                    (2, 40, '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id	val	doubled_running
                            1	10	20.0
                            1	20	60.0
                            2	30	60.0
                            2	40	140.0
                            """,
                    """
                            SELECT t1.id, sub.val, sub.doubled_running
                            FROM t1
                            JOIN LATERAL (
                                SELECT val, (sum(val) OVER (ORDER BY ts)) * 2 AS doubled_running
                                FROM t2
                                WHERE t2.t1_id = t1.id
                            ) sub
                            ORDER BY t1.id, sub.val
                            """,
                    null, true, true
            );
        });
    }

    // T74: Window function inside UNION branch of lateral
    @Test
    public void testT74WindowInUnionBranch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t3 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t3 VALUES
                    (2, 30, '2024-01-01T01:10:00.000000Z'),
                    (2, 40, '2024-01-01T01:20:00.000000Z')
                    """);

            // Each UNION branch has its own row_number() window function
            assertQueryNoLeakCheck(
                    """
                            id\tval\trn
                            1\t10\t1
                            1\t20\t2
                            2\t30\t1
                            2\t40\t2
                            """,
                    """
                            SELECT t1.id, sub.val, sub.rn
                            FROM t1
                            JOIN LATERAL (
                                SELECT val, row_number() OVER (ORDER BY ts) AS rn
                                FROM t2
                                WHERE t2.t1_id = t1.id
                                UNION ALL
                                SELECT val, row_number() OVER (ORDER BY ts) AS rn
                                FROM t3
                                WHERE t3.t1_id = t1.id
                            ) sub
                            ORDER BY t1.id, sub.val
                            """,
                    null, true, true
            );
        });
    }

    // T75: SELECT * with simple lateral — outer ref elimination + wildcard handling
    @Test
    public void testT75WildcardWithOuterRefElimination() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_a INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 30, '2024-01-01T01:10:00.000000Z')
                    """);

            // SELECT * must not expose __qdb_outer_ref__ columns
            assertQueryNoLeakCheck(
                    """
                            a\tts\tval\tts1
                            1\t2024-01-01T00:00:00.000000Z\t10\t2024-01-01T00:10:00.000000Z
                            1\t2024-01-01T00:00:00.000000Z\t20\t2024-01-01T00:20:00.000000Z
                            2\t2024-01-01T01:00:00.000000Z\t30\t2024-01-01T01:10:00.000000Z
                            """,
                    """
                            SELECT *
                            FROM t1
                            JOIN LATERAL (
                                SELECT val, ts FROM t2 WHERE t2.t1_a = t1.a
                            ) sub
                            ORDER BY t1.a, sub.val
                            """,
                    null, true, false
            );
        });
    }

    // T76: UNION branch with non-eq correlation blocks outer ref elimination
    @Test
    public void testT76UnionBranchBlocksElimination() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t3 (threshold INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t3 VALUES
                    (0, 100, '2024-01-01T00:00:00.000000Z'),
                    (1, 200, '2024-01-01T00:00:01.000000Z'),
                    (2, 300, '2024-01-01T00:00:02.000000Z')
                    """);

            // Branch 1: equality correlation (can eliminate)
            // Branch 2: non-equality correlation t3.threshold < t1.id (blocks elimination)
            // id=1: t2 val=10; t3 threshold<1 → threshold=0(val=100)
            // id=2: t2 val=20; t3 threshold<2 → threshold=0(val=100), threshold=1(val=200)
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\t10
                            1\t100
                            2\t20
                            2\t100
                            2\t200
                            """,
                    """
                            SELECT t1.id, sub.val
                            FROM t1
                            JOIN LATERAL (
                                SELECT val FROM t2 WHERE t2.t1_id = t1.id
                                UNION ALL
                                SELECT val FROM t3 WHERE t3.threshold < t1.id
                            ) sub
                            ORDER BY t1.id, sub.val
                            """,
                    null, true, true
            );
        });
    }

    // T77: LATEST BY inside UNION branch of lateral
    @Test
    public void testT77LatestByInUnionBranch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (order_id INT, category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (order_id INT, category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_a VALUES
                    (1, 'X', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 'X', 15.0, '2024-01-01T00:20:00.000000Z'),
                    (1, 'Y', 5.0,  '2024-01-01T00:30:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_b VALUES
                    (2, 'X', 20.0, '2024-01-01T01:10:00.000000Z'),
                    (2, 'Y', 25.0, '2024-01-01T01:20:00.000000Z'),
                    (2, 'Y', 30.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // LATEST BY inside each UNION branch: keeps last row per category
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\tqty
                            1\tX\t15.0
                            1\tY\t5.0
                            2\tX\t20.0
                            2\tY\t30.0
                            """,
                    """
                            SELECT o.id, sub.category, sub.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, qty FROM trades_a
                                WHERE order_id = o.id
                                LATEST ON ts PARTITION BY category
                                UNION ALL
                                SELECT category, qty FROM trades_b
                                WHERE order_id = o.id
                                LATEST ON ts PARTITION BY category
                            ) sub
                            ORDER BY o.id, sub.category
                            """,
                    null, true, true
            );
        });
    }

    // T77b: LATEST BY + LEFT lateral — unmatched outer rows get NULL
    @Test
    public void testT77bLatestByLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 'A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 'A', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (1, 'B', 30.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 'A', 40.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // order 1: latest A=20, latest B=30
            // order 2: latest A=40
            // order 3: no trades → NULL fill
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\tqty
                            1\tA\t20.0
                            1\tB\t30.0
                            2\tA\t40.0
                            3\t\tnull
                            """,
                    """
                            SELECT o.id, t.category, t.qty
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT category, qty FROM trades
                                WHERE order_id = o.id
                                LATEST ON ts PARTITION BY category
                            ) t
                            ORDER BY o.id, t.category
                            """,
                    null, true, false
            );
        });
    }

    // T77c: LATEST BY + multiple correlation columns (fast path with 2 physical cols)
    @Test
    public void testT77cLatestByMultiCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE markets (mm_id SYMBOL, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE quotes (mm_id SYMBOL, symbol SYMBOL, side SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO markets VALUES
                    ('mm1', 'AAPL', '2024-01-01T00:00:00.000000Z'),
                    ('mm2', 'AAPL', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO quotes VALUES
                    ('mm1', 'AAPL', 'BID', 99.0,  '2024-01-01T00:10:00.000000Z'),
                    ('mm1', 'AAPL', 'BID', 100.0, '2024-01-01T00:20:00.000000Z'),
                    ('mm1', 'AAPL', 'ASK', 101.0, '2024-01-01T00:30:00.000000Z'),
                    ('mm2', 'AAPL', 'BID', 98.0,  '2024-01-01T01:10:00.000000Z'),
                    ('mm2', 'AAPL', 'ASK', 102.0, '2024-01-01T01:20:00.000000Z'),
                    ('mm2', 'AAPL', 'ASK', 103.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // Two correlation columns: mm_id + symbol
            // mm1/AAPL: latest BID=100, latest ASK=101
            // mm2/AAPL: latest BID=98, latest ASK=103
            assertQueryNoLeakCheck(
                    """
                            mm_id\tsymbol\tside\tprice
                            mm1\tAAPL\tASK\t101.0
                            mm1\tAAPL\tBID\t100.0
                            mm2\tAAPL\tASK\t103.0
                            mm2\tAAPL\tBID\t98.0
                            """,
                    """
                            SELECT m.mm_id, m.symbol, q.side, q.price
                            FROM markets m
                            JOIN LATERAL (
                                SELECT side, price FROM quotes
                                WHERE mm_id = m.mm_id AND symbol = m.symbol
                                LATEST ON ts PARTITION BY side
                            ) q
                            ORDER BY m.mm_id, q.side
                            """,
                    null, true, false
            );
        });
    }

    // T77d: LATEST BY + non-eq correlation → fallback to window function
    @Test
    public void testT77dLatestByNonEqFallback() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, start_ts TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:15:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    ('A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    ('A', 20.0, '2024-01-01T00:20:00.000000Z'),
                    ('B', 30.0, '2024-01-01T00:30:00.000000Z'),
                    ('A', 40.0, '2024-01-01T01:10:00.000000Z'),
                    ('B', 50.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Non-eq correlation: ts > o.start_ts (no equality → fallback to window func)
            // order 1 (start_ts=00:15): trades after 00:15 → A@00:20(20), B@00:30(30)
            //   latest per category: A=20, B=30
            // order 2 (start_ts=01:00): trades after 01:00 → A@01:10(40), B@01:20(50)
            //   latest per category: A=40, B=50
            assertQueryNoLeakCheck(
                    """
                            id	category	qty
                            1	A	40.0
                            1	B	50.0
                            2	A	40.0
                            2	B	50.0
                            """,
                    """
                            SELECT o.id, t.category, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, qty FROM trades
                                WHERE ts > o.start_ts
                                LATEST ON ts PARTITION BY category
                            ) t
                            ORDER BY o.id, t.category
                            """,
                    null, true, false
            );
        });
    }

    // T77e1: LATEST BY PARTITION BY references outer column directly (no WHERE correlation)
    // Correlation is ONLY in PARTITION BY (o.id), not in WHERE → fallback to window function
    @Test
    public void testT77e1LatestByPartitionByOuterCol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // PARTITION BY o.id → outer column in PARTITION BY, no WHERE
            // For each order, all trades are considered, grouped by o.id
            // Since o.id is constant per lateral execution, each order gets the latest trade overall
            // order 1: latest of ALL trades = qty=30 (ts=01:10)
            // order 2: latest of ALL trades = qty=30 (ts=01:10)
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t30.0
                            2\t30.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades
                                LATEST ON ts PARTITION BY id
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T77e: LATEST BY + eq + non-eq mixed correlation → fallback to window function
    @Test
    public void testT77eLatestByMixedCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, min_ts TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:15:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:15:00.000000Z', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 'A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 'A', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (1, 'B', 30.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 'A', 40.0, '2024-01-01T01:10:00.000000Z'),
                    (2, 'A', 50.0, '2024-01-01T01:20:00.000000Z'),
                    (2, 'B', 60.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // eq: order_id = o.id (→ fast path, add order_id to PARTITION BY)
            // non-eq: ts > o.min_ts (stays in WHERE as filter)
            // order 1 (min_ts=00:15): trades with order_id=1 AND ts>00:15 → A@00:20(20), B@00:30(30)
            //   latest per category: A=20, B=30
            // order 2 (min_ts=01:15): trades with order_id=2 AND ts>01:15 → A@01:20(50), B@01:30(60)
            //   latest per category: A=50, B=60
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\tqty
                            1\tA\t20.0
                            1\tB\t30.0
                            2\tA\t50.0
                            2\tB\t60.0
                            """,
                    """
                            SELECT o.id, t.category, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, qty FROM trades
                                WHERE order_id = o.id AND ts > o.min_ts
                                LATEST ON ts PARTITION BY category
                            ) t
                            ORDER BY o.id, t.category
                            """,
                    null, true, false
            );
        });
    }

    // T77f: LATEST BY + LEFT + non-eq fallback — unmatched rows get NULL
    @Test
    public void testT77fLatestByLeftNonEqFallback() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, start_ts TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:15:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T23:00:00.000000Z', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    ('A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    ('A', 20.0, '2024-01-01T00:20:00.000000Z'),
                    ('B', 30.0, '2024-01-01T00:30:00.000000Z')
                    """);

            // order 1 (start_ts=00:15): trades after 00:15 → A=20, B=30
            // order 2 (start_ts=23:00): no trades after 23:00 → NULL
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\tqty
                            1\tA\t20.0
                            1\tB\t30.0
                            2\t\tnull
                            """,
                    """
                            SELECT o.id, t.category, t.qty
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT category, qty FROM trades
                                WHERE ts > o.start_ts
                                LATEST ON ts PARTITION BY category
                            ) t
                            ORDER BY o.id, t.category
                            """,
                    null, true, false
            );
        });
    }

    // T78: LEFT lateral with empty outer table — zero rows propagated
    @Test
    public void testT78EmptyOuterTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t2 VALUES (1, 10, '2024-01-01T00:10:00.000000Z')");

            // Empty outer with INNER → 0 rows
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            """,
                    """
                            SELECT t1.id, sub.val
                            FROM t1
                            JOIN LATERAL (SELECT val FROM t2 WHERE t2.t1_id = t1.id) sub
                            ORDER BY t1.id
                            """,
                    null, true, false
            );

            // Empty outer with LEFT → 0 rows (LEFT doesn't create rows from nothing)
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            """,
                    """
                            SELECT t1.id, sub.val
                            FROM t1
                            LEFT JOIN LATERAL (SELECT val FROM t2 WHERE t2.t1_id = t1.id) sub
                            ORDER BY t1.id
                            """,
                    null, true, false
            );
        });
    }

    // T79: Correlated ref only in ORDER BY (not in WHERE, SELECT, or GROUP BY)
    @Test
    public void testT79CorrelatedRefInOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, sort_dir INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 1, '2024-01-01T00:00:00.000000Z'),
                    (2, -1, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 30, '2024-01-01T01:10:00.000000Z'),
                    (2, 40, '2024-01-01T01:20:00.000000Z')
                    """);

            // ORDER BY uses outer column in expression: val * t1.sort_dir
            // id=1 sort_dir=1: order by val*1 ASC → 10, 20
            // id=2 sort_dir=-1: order by val*(-1) ASC → 40, 30 (since -40 < -30)
            assertQueryNoLeakCheck(
                    """
                            id	val
                            1	20
                            1	10
                            2	30
                            2	40
                            """,
                    """
                            SELECT t1.id, sub.val
                            FROM t1
                            JOIN LATERAL (
                                SELECT val FROM t2
                                WHERE t2.t1_id = t1.id
                                ORDER BY val * t1.sort_dir
                            ) sub
                            """,
                    null, false, false
            );
        });
    }

    // T80: Window function compensation — multiple windows with different ORDER BY
    @Test
    public void testT80WindowCompensation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 30.0, '2024-01-01T00:20:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 50.0, '2024-01-01T01:10:00.000000Z'),
                    (2, 40.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Two window functions with different ORDER BY inside lateral
            // rn_ts: rank by timestamp, rn_qty: rank by qty DESC
            assertQueryNoLeakCheck(
                    """
                            id\tqty\trn_ts\trn_qty
                            1\t10.0\t1\t3
                            1\t30.0\t2\t1
                            1\t20.0\t3\t2
                            2\t50.0\t1\t1
                            2\t40.0\t2\t2
                            """,
                    """
                            SELECT o.id, sub.qty, sub.rn_ts, sub.rn_qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty,
                                       row_number() OVER (ORDER BY ts) AS rn_ts,
                                       row_number() OVER (ORDER BY qty DESC) AS rn_qty
                                FROM trades
                                WHERE order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.rn_ts
                            """,
                    null, true, false
            );
        });
    }

    // T80b: Complex window functions — running sum, lag, rank with PARTITION BY + frame
    @Test
    public void testT80bComplexWindowFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, category SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, category SYMBOL, qty DOUBLE, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'A', '2024-01-01T00:00:00.000000Z'),
                    (2, 'B', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 'A', 10.0, 100.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 'A', 20.0, 200.0, '2024-01-01T00:20:00.000000Z'),
                    (1, 'A', 30.0, 150.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 'B', 40.0, 300.0, '2024-01-01T01:10:00.000000Z'),
                    (2, 'B', 50.0, 250.0, '2024-01-01T01:20:00.000000Z'),
                    (2, 'B', 60.0, 350.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // Multiple complex window functions:
            // running_qty: cumulative sum of qty
            // prev_price: lag(price, 1) — previous trade's price (null for first)
            // price_rank: dense_rank by price DESC
            // avg_qty: running average of qty
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tprice\trunning_qty\tprev_price\tprice_rank\tavg_qty
                            1\t10.0\t100.0\t10.0\tnull\t3\t10.0
                            1\t20.0\t200.0\t30.0\t100.0\t1\t15.0
                            1\t30.0\t150.0\t60.0\t200.0\t2\t20.0
                            2\t40.0\t300.0\t40.0\tnull\t2\t40.0
                            2\t50.0\t250.0\t90.0\t300.0\t3\t45.0
                            2\t60.0\t350.0\t150.0\t250.0\t1\t50.0
                            """,
                    """
                            SELECT o.id, sub.qty, sub.price, sub.running_qty,
                                   sub.prev_price, sub.price_rank, sub.avg_qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty, price,
                                       sum(qty) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
                                       lag(price, 1) OVER (ORDER BY ts) AS prev_price,
                                       dense_rank() OVER (ORDER BY price DESC) AS price_rank,
                                       avg(qty) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_qty
                                FROM trades
                                WHERE order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.qty
                            """,
                    null, true, false
            );
        });
    }

    // T80c: Window function with explicit PARTITION BY inside lateral
    @Test
    public void testT80cWindowWithPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, side SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 'BUY',  10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 'SELL', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (1, 'BUY',  30.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 'BUY',  40.0, '2024-01-01T01:10:00.000000Z'),
                    (2, 'BUY',  50.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Window with user-specified PARTITION BY side — decorrelation must
            // add outer ref grouping cols to PARTITION BY alongside the user's column
            // rn: row_number per side within each order
            // side_total: running sum per side within each order
            assertQueryNoLeakCheck(
                    """
                            id\tside\tqty\trn\tside_total
                            1\tBUY\t10.0\t1\t10.0
                            1\tBUY\t30.0\t2\t40.0
                            1\tSELL\t20.0\t1\t20.0
                            2\tBUY\t40.0\t1\t40.0
                            2\tBUY\t50.0\t2\t90.0
                            """,
                    """
                            SELECT o.id, sub.side, sub.qty, sub.rn, sub.side_total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT side, qty,
                                       row_number() OVER (PARTITION BY side ORDER BY ts) AS rn,
                                       sum(qty) OVER (PARTITION BY side ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS side_total
                                FROM trades
                                WHERE order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.side, sub.qty
                            """,
                    null, true, true
            );
        });
    }

    // T81: JOIN inside UNION inside lateral
    @Test
    public void testT81JoinInsideUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE refunds (order_id INT, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE labels (order_id INT, label SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO refunds VALUES
                    (1, 5.0, '2024-01-01T00:20:00.000000Z')
                    """);
            execute("""
                    INSERT INTO labels VALUES
                    (1, 'trade', '2024-01-01T00:10:00.000000Z'),
                    (2, 'trade', '2024-01-01T01:10:00.000000Z'),
                    (1, 'refund', '2024-01-01T00:20:00.000000Z')
                    """);

            // UNION branch 1: trades JOIN labels (both correlated)
            // UNION branch 2: refunds JOIN labels (both correlated)
            assertQueryNoLeakCheck(
                    """
                            id\tval\tlabel
                            1\t10.0\ttrade
                            1\t5.0\trefund
                            2\t20.0\ttrade
                            """,
                    """
                            SELECT o.id, sub.val, sub.label
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty AS val, l.label
                                FROM trades t
                                JOIN labels l ON l.order_id = t.order_id AND l.label = 'trade'
                                WHERE t.order_id = o.id
                                UNION ALL
                                SELECT r.amount AS val, l.label
                                FROM refunds r
                                JOIN labels l ON l.order_id = r.order_id AND l.label = 'refund'
                                WHERE r.order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.val DESC
                            """,
                    null, true, true
            );
        });
    }

    // T82: UNION inside JOIN inside lateral
    @Test
    public void testT82UnionInsideJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE tags (order_id INT, tag SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("INSERT INTO trades_a VALUES (1, 10.0, '2024-01-01T00:10:00.000000Z')");
            execute("INSERT INTO trades_b VALUES (1, 20.0, '2024-01-01T00:20:00.000000Z'), (2, 30.0, '2024-01-01T01:10:00.000000Z')");
            execute("""
                    INSERT INTO tags VALUES
                    (1, 'VIP', '2024-01-01T00:00:00.000000Z'),
                    (2, 'STD', '2024-01-01T01:00:00.000000Z')
                    """);

            // Inner UNION (trades_a + trades_b) cross joined with tags — both correlated to outer
            assertQueryNoLeakCheck(
                    """
                            id\ttotal_qty\ttag
                            1\t30.0\tVIP
                            2\t30.0\tSTD
                            """,
                    """
                            SELECT o.id, sub.total_qty, sub.tag
                            FROM orders o
                            JOIN LATERAL (
                                SELECT u.total_qty, tg.tag
                                FROM (
                                    SELECT sum(qty) AS total_qty FROM (
                                        SELECT qty FROM trades_a WHERE order_id = o.id
                                        UNION ALL
                                        SELECT qty FROM trades_b WHERE order_id = o.id
                                    )
                                ) u
                                CROSS JOIN (SELECT tag FROM tags WHERE order_id = o.id) tg
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    // T83: JOIN with mixed correlation — some branches correlated, some not
    @Test
    public void testT83JoinMixedCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, product_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE products (id INT, name STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 100, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 200, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 100, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO products VALUES
                    (100, 'Apple', '2024-01-01T00:00:00.000000Z'),
                    (200, 'Banana', '2024-01-01T00:00:01.000000Z')
                    """);

            // trades: correlated (order_id = o.id)
            // products: NOT correlated (joined to trades, not to outer)
            assertQueryNoLeakCheck(
                    """
                            id\tname\tqty
                            1\tApple\t10.0
                            1\tBanana\t20.0
                            2\tApple\t30.0
                            """,
                    """
                            SELECT o.id, sub.name, sub.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT p.name, t.qty
                                FROM trades t
                                JOIN products p ON p.id = t.product_id
                                WHERE t.order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.name
                            """,
                    null, true, true
            );
        });
    }

    // T84: UNION with mixed correlation — one branch correlated, other uncorrelated
    @Test
    public void testT84UnionMixedCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // Branch 1: correlated (order_id = o.id)
            // Branch 2: uncorrelated constant row
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\t0.0
                            1\t10.0
                            2\t0.0
                            2\t20.0
                            """,
                    """
                            SELECT o.id, sub.val
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty AS val FROM trades WHERE order_id = o.id
                                UNION ALL
                                SELECT 0 AS val FROM long_sequence(1)
                            ) sub
                            ORDER BY o.id, sub.val
                            """,
                    null, true, true
            );
        });
    }

    // T85: ASOF JOIN inside lateral
    @Test
    public void testT85AsofJoinInsideLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE prices (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO prices VALUES
                    (100.0, '2024-01-01T00:05:00.000000Z'),
                    (101.0, '2024-01-01T00:15:00.000000Z'),
                    (102.0, '2024-01-01T01:05:00.000000Z')
                    """);

            // ASOF JOIN inside lateral: each trade gets the most recent price at trade time
            // trade(qty=10,ts=00:10) asof prices → price=100 (at 00:05)
            // trade(qty=20,ts=00:30) asof prices → price=101 (at 00:15)
            // trade(qty=30,ts=01:10) asof prices → price=102 (at 01:05)
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tprice
                            1\t10.0\t100.0
                            1\t20.0\t101.0
                            2\t30.0\t102.0
                            """,
                    """
                            SELECT o.id, sub.qty, sub.price
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, p.price
                                FROM trades t
                                ASOF JOIN prices p
                                WHERE t.order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.qty
                            """,
                    null, true, true
            );

            assertQueryNoLeakCheck(
                    """
                            id	qty	price	rn
                            1	10.0	100.0	2
                            1	20.0	101.0	1
                            2	30.0	102.0	1
                            """,
                    """
                            SELECT o.id, sub.qty, sub.price, sub.rn
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, p.price, row_number() OVER (ORDER BY p.ts DESC) AS rn
                                FROM trades t
                                ASOF JOIN prices p
                                WHERE t.order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.qty
                            """,
                    null, true, false
            );
        });
    }

    // T86: Correlated JOIN ON with uncorrelated branches
    @Test
    public void testT86CorrelatedJoinOnUncorrelatedBranches() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, category SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE limits (category SYMBOL, max_qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'A', '2024-01-01T00:00:00.000000Z'),
                    (2, 'B', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    ('A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    ('A', 20.0, '2024-01-01T00:20:00.000000Z'),
                    ('B', 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO limits VALUES
                    ('A', 15.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 50.0, '2024-01-01T00:00:01.000000Z')
                    """);

            // JOIN ON has correlation (o.category) but both branches (trades, limits) are uncorrelated
            // Only the JOIN condition references the outer table
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tmax_qty
                            1\t10.0\t15.0
                            1\t20.0\t15.0
                            2\t30.0\t50.0
                            """,
                    """
                            SELECT o.id, sub.qty, sub.max_qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, l.max_qty
                                FROM trades t
                                JOIN limits l ON l.category = t.category
                                WHERE t.category = o.category
                            ) sub
                            ORDER BY o.id, sub.qty
                            """,
                    null, true, true
            );
        });
    }

    // T87: Correlated JOIN ON + correlated branch — both criteria and branch have correlation
    @Test
    public void testT87CorrelatedJoinOnAndBranch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, min_qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE factors (trade_order_id INT, factor DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 15.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 5.0,  '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO factors VALUES
                    (1, 1.5, '2024-01-01T00:00:00.000000Z'),
                    (2, 2.0, '2024-01-01T01:00:00.000000Z')
                    """);

            // trades: correlated WHERE (order_id = o.id AND qty > o.min_qty)
            // factors: joined to trades via non-correlated ON
            // WHERE has both eq correlation (order_id) and non-eq correlation (qty > min_qty)
            // order 1 (min_qty=15): only qty=20 passes ON condition, factor=1.5, result=30
            // order 2 (min_qty=5): only qty=30 passes ON condition, factor=2.0, result=60
            assertQueryNoLeakCheck(
                    """
                            id\tadjusted_qty
                            1\t30.0
                            2\t60.0
                            """,
                    """
                            SELECT o.id, sub.adjusted_qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty * f.factor AS adjusted_qty
                                FROM trades t
                                JOIN factors f ON f.trade_order_id = t.order_id
                                WHERE t.order_id = o.id AND t.qty > o.min_qty
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testT87bCommaLateral() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                """
                        x\ty\tz
                        1\t2\t2
                        """,
                """
                        SELECT ss1.x, ss2.y, ss3.z FROM
                          (SELECT 1 AS x) ss1
                          LEFT JOIN (SELECT 2 AS y) ss2 ON (true),
                          LATERAL (SELECT ss2.y AS z FROM long_sequence(1) LIMIT 1) ss3
                        """,
                null, false, true
        ));
    }

    // T88: Unqualified correlated ref — exercises rewriteOuterRefs no-dot fallback
    @Test
    public void testT88UnqualifiedCorrelatedRef() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_a INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t1 VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z')
                    """);

            // 'a' in WHERE is unqualified — resolves to t1.a via no-dot fallback
            assertQueryNoLeakCheck(
                    """
                            a\tval
                            1\t10
                            2\t20
                            """,
                    """
                            SELECT t1.a, sub.val
                            FROM t1
                            JOIN LATERAL (
                                SELECT val FROM t2 WHERE t1_a = a
                            ) sub
                            ORDER BY t1.a
                            """,
                    null, true, false
            );
        });
    }

    // T89: GROUP BY in UNION branch inside lateral
    @Test
    public void testT89GroupByInUnionBranch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE refunds (order_id INT, category SYMBOL, amt DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 'X', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 'X', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 'Y', 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO refunds VALUES
                    (1, 'X', 5.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 'Y', 8.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // UNION: branch 1 has GROUP BY, branch 2 has GROUP BY
            // Both need groupingCols added to their GROUP BY
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\ttotal
                            1\tX\t30.0
                            1\tX\t5.0
                            2\tY\t30.0
                            2\tY\t8.0
                            """,
                    """
                            SELECT o.id, sub.category, sub.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, sum(qty) AS total FROM trades
                                WHERE order_id = o.id GROUP BY category
                                UNION ALL
                                SELECT category, sum(amt) AS total FROM refunds
                                WHERE order_id = o.id GROUP BY category
                            ) sub
                            ORDER BY o.id, sub.total DESC
                            """,
                    null, true, true
            );
        });
    }

    // T90: LIMIT + offset with GROUP BY (exercises compensateLimit wrapping path)
    @Test
    public void testT90LimitOffsetWithGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 'A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 'B', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (1, 'C', 30.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 'A', 40.0, '2024-01-01T01:10:00.000000Z'),
                    (2, 'B', 50.0, '2024-01-01T01:20:00.000000Z'),
                    (2, 'C', 60.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // GROUP BY + ORDER BY total DESC + LIMIT 1, 2 (skip top-1, take next 2)
            // order 1: A=10, B=20, C=30 → sorted DESC: C(30), B(20), A(10) → skip 1, take 2: B(20), A(10)
            // order 2: A=40, B=50, C=60 → sorted DESC: C(60), B(50), A(40) → skip 1, take 2: B(50), A(40)
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\ttotal
                            1\tA\t10.0
                            1\tB\t20.0
                            2\tA\t40.0
                            2\tB\t50.0
                            """,
                    """
                            SELECT o.id, sub.category, sub.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, sum(qty) AS total
                                FROM trades WHERE order_id = o.id
                                GROUP BY category
                                ORDER BY total DESC
                                LIMIT 1, 3
                            ) sub
                            ORDER BY o.id, sub.total
                            """,
                    null, true, false
            );
        });
    }

    // T91: Subquery as outer table — exercises createOuterRefBase deepClone path
    @Test
    public void testT91SubqueryOuter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, status SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'active', '2024-01-01T00:00:00.000000Z'),
                    (2, 'closed', '2024-01-01T01:00:00.000000Z'),
                    (3, 'active', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 30.0, '2024-01-01T02:10:00.000000Z')
                    """);

            // Outer is a subquery (not a bare table) — createOuterRefBase
            // takes the nestedModel path and deep-clones it
            assertQueryNoLeakCheck(
                    """
                            id\ttotal
                            1\t30.0
                            3\t30.0
                            """,
                    """
                            SELECT o.id, sub.total
                            FROM (SELECT id, ts FROM orders WHERE status = 'active') o
                            JOIN LATERAL (
                                SELECT sum(qty) AS total FROM trades WHERE order_id = o.id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T92: SELECT * with mixed eq/non-eq correlation — wildcard + partial elimination
    @Test
    public void testT92WildcardMixedCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, threshold INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 15, '2024-01-01T00:00:00.000000Z'),
                    (2, 25, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 30, '2024-01-01T01:10:00.000000Z'),
                    (2, 40, '2024-01-01T01:20:00.000000Z')
                    """);

            // SELECT * + eq correlation (t1_id = id) + non-eq (val > threshold)
            // id=1, threshold=15: val>15 → val=20
            // id=2, threshold=25: val>25 → val=30, val=40
            // __qdb_outer_ref__ columns must NOT appear in output
            assertQueryNoLeakCheck(
                    """
                            id\tthreshold\tts\tval\tts1
                            1\t15\t2024-01-01T00:00:00.000000Z\t20\t2024-01-01T00:20:00.000000Z
                            2\t25\t2024-01-01T01:00:00.000000Z\t30\t2024-01-01T01:10:00.000000Z
                            2\t25\t2024-01-01T01:00:00.000000Z\t40\t2024-01-01T01:20:00.000000Z
                            """,
                    """
                            SELECT *
                            FROM t1
                            JOIN LATERAL (
                                SELECT val, ts FROM t2
                                WHERE t2.t1_id = t1.id AND t2.val > t1.threshold
                            ) sub
                            ORDER BY t1.id, sub.val
                            """,
                    null, true, false
            );
        });
    }

    // T93: Correlated subquery inside lateral WHERE (IN subquery)
    @Test
    public void testT93CorrelatedSubqueryInLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE valid_orders (order_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 20.0, '2024-01-01T01:10:00.000000Z'),
                    (3, 30.0, '2024-01-01T02:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO valid_orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);

            assertException(
                    """
                            SELECT o.id, sub.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades
                                WHERE order_id = o.id
                                  AND order_id IN (SELECT order_id FROM valid_orders)
                            ) sub
                            ORDER BY o.id
                            """,
                    126,
                    "cannot compare LONG with type CURSOR"
            );
        });
    }

    // T94: Lateral with DISTINCT + GROUP BY on same query (compensateDistinct + compensateAggregate)
    @Test
    public void testT94DistinctWithGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 'A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 'A', 10.0, '2024-01-01T00:20:00.000000Z'),
                    (1, 'B', 20.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 'A', 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // DISTINCT on top of GROUP BY — both compensations must add groupingCols
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\ttotal
                            1\tA\t20.0
                            1\tB\t20.0
                            2\tA\t30.0
                            """,
                    """
                            SELECT o.id, sub.category, sub.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT DISTINCT category, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                GROUP BY category
                            ) sub
                            ORDER BY o.id, sub.category
                            """,
                    null, true, true
            );
        });
    }

    // T95: LEFT LATERAL count — outer table has column with same name as count alias
    // applyLateralCountCoalesce must NOT wrap t1.cnt with coalesce
    @Test
    public void testT95LeftCountAliasClashWithOuterColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, cnt INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 100, '2024-01-01T00:00:00.000000Z'),
                    (2, 200, '2024-01-01T01:00:00.000000Z'),
                    (3, 300, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 30, '2024-01-01T01:10:00.000000Z')
                    """);

            // t1.cnt is a regular column (100, 200, 300), sub.cnt is count(*)
            // Only sub.cnt should become coalesce(sub.cnt, 0) for the LEFT unmatched row
            assertQueryNoLeakCheck(
                    """
                            cnt\tcnt1
                            100\t2
                            200\t1
                            300\t0
                            """,
                    """
                            SELECT t1.cnt, sub.cnt
                            FROM t1
                            LEFT JOIN LATERAL (
                                SELECT count(*) AS cnt FROM t2 WHERE t2.t1_id = t1.id
                            ) sub ON true
                            ORDER BY t1.id
                            """,
                    null, true, false
            );
        });
    }

    // T95b: Same as T95 but with wildcard SELECT *
    @Test
    public void testT95bLeftCountAliasClashWithOuterColumnWildcard() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, cnt INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 100, '2024-01-01T00:00:00.000000Z'),
                    (2, 200, '2024-01-01T01:00:00.000000Z'),
                    (3, 300, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 30, '2024-01-01T01:10:00.000000Z')
                    """);

            // With SELECT *, wildcard expansion may rename sub.cnt to cnt1
            // Only the lateral count column should get coalesce, not t1.cnt
            assertQueryNoLeakCheck(
                    """
                            id\tcnt\tts\tcnt1
                            1\t100\t2024-01-01T00:00:00.000000Z\t2
                            2\t200\t2024-01-01T01:00:00.000000Z\t1
                            3\t300\t2024-01-01T02:00:00.000000Z\t0
                            """,
                    """
                            SELECT *
                            FROM t1
                            LEFT JOIN LATERAL (
                                SELECT count(*) AS cnt FROM t2 WHERE t2.t1_id = t1.id
                            ) sub ON true
                            ORDER BY t1.id
                            """,
                    null, true, false
            );
        });
    }

    // T96: LEFT LATERAL with multiple count columns + wildcard
    @Test
    public void testT96LeftMultipleCountColumnsWildcard() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, category SYMBOL, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 'A', 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 'A', 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 'B', 30, '2024-01-01T01:10:00.000000Z')
                    """);

            // Two count aggregates: cnt_all and cnt_cat. Both must be 0 for unmatched row.
            assertQueryNoLeakCheck(
                    """
                            id\tts\tcategory\tcnt_all\tcnt_cat
                            1\t2024-01-01T00:00:00.000000Z\tA\t2\t2
                            2\t2024-01-01T01:00:00.000000Z\tB\t1\t1
                            3\t2024-01-01T02:00:00.000000Z\t\t0\t0
                            """,
                    """
                            SELECT *
                            FROM t1
                            LEFT JOIN LATERAL (
                                SELECT category, count(*) AS cnt_all, count(category) AS cnt_cat
                                FROM t2
                                WHERE t2.t1_id = t1.id
                                GROUP BY category
                            ) sub ON true
                            ORDER BY t1.id
                            """,
                    null, true, false
            );
        });
    }

    // T96b: LEFT LATERAL count with explicit column list (non-wildcard)
    @Test
    public void testT96bLeftCountExplicitColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 30, '2024-01-01T01:10:00.000000Z')
                    """);

            // Non-wildcard: explicit t1.id and sub.cnt columns
            assertQueryNoLeakCheck(
                    """
                            id\tcnt\ttotal
                            1\t2\t30
                            2\t1\t30
                            3\t0\tnull
                            """,
                    """
                            SELECT t1.id, sub.cnt, sub.total
                            FROM t1
                            LEFT JOIN LATERAL (
                                SELECT count(*) AS cnt, sum(val) AS total
                                FROM t2
                                WHERE t2.t1_id = t1.id
                            ) sub ON true
                            ORDER BY t1.id
                            """,
                    null, true, false
            );
        });
    }

    // T97: LEFT LATERAL count + sum with wildcard, no GROUP BY (scalar aggregate)
    @Test
    public void testT97LeftCountAndSumWildcard() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 30, '2024-01-01T01:10:00.000000Z')
                    """);

            // count(*) should be 0 for unmatched, sum should be null
            assertQueryNoLeakCheck(
                    """
                            id\tts\tcnt\ttotal
                            1\t2024-01-01T00:00:00.000000Z\t2\t30
                            2\t2024-01-01T01:00:00.000000Z\t1\t30
                            3\t2024-01-01T02:00:00.000000Z\t0\tnull
                            """,
                    """
                            SELECT *
                            FROM t1
                            LEFT JOIN LATERAL (
                                SELECT count(*) AS cnt, sum(val) AS total
                                FROM t2
                                WHERE t2.t1_id = t1.id
                            ) sub ON true
                            ORDER BY t1.id
                            """,
                    null, true, false
            );
        });
    }

    // T97: LIMIT 0 inside lateral — should return no rows per group
    @Test
    public void testT97LimitZero() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // LIMIT 0 should produce zero rows → INNER join produces empty result
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            """,
                    """
                            SELECT o.id, sub.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades
                                WHERE order_id = o.id
                                ORDER BY qty
                                LIMIT 0
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T98: NULL in non-equality correlation predicate
    @Test
    public void testT98NullInNonEqualityCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, threshold DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 15.0, '2024-01-01T00:00:00.000000Z'),
                    (2, NULL, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // NULL threshold: qty > NULL is false, so order 2 matches no trades
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t20.0
                            """,
                    """
                            SELECT o.id, sub.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades
                                WHERE order_id = o.id AND qty > o.threshold
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T98: outer WHERE filter + non-equality lateral correlation.
    @Test
    public void testT98OuterWhereFilterWithNonEqLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, status SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'ACTIVE', '2024-01-01T00:00:00.000000Z'),
                    (2, 'CLOSED', '2024-01-01T01:00:00.000000Z'),
                    (3, 'ACTIVE', '2024-01-01T02:00:00.000000Z'),
                    (4, 'ACTIVE', '2024-01-01T03:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z'),
                    (3, 40.0, '2024-01-01T02:10:00.000000Z')
                    """);

            // Non-equality correlation (order_id > o.id) prevents elimination
            // Only ACTIVE orders are included
            assertQueryNoLeakCheck(
                    """
                            id\tcnt
                            1\t2
                            3\t1
                            4\t0
                            """,
                    """
                            SELECT o.id, sub.cnt
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT count(*) AS cnt
                                FROM trades
                                WHERE order_id >= o.id AND order_id < o.id + 1
                            ) sub ON true
                            WHERE o.status = 'ACTIVE'
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T98b: single source + unqualified WHERE column name
    // Regression test: canResolveColumnForOuter must resolve unqualified columns
    @Test
    public void testT98bOuterWhereUnqualifiedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, status SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'ACTIVE', '2024-01-01T00:00:00.000000Z'),
                    (2, 'CLOSED', '2024-01-01T01:00:00.000000Z'),
                    (3, 'ACTIVE', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // Unqualified 'status' (no alias prefix) — must still be pushed down
            assertQueryAndPlan(
                    """
                            id\tcnt
                            1\t2
                            3\t0
                            """,
                    """
                            Encode sort
                              keys: [id]
                                VirtualRecord
                                  functions: [id,coalesce(cnt,0)]
                                    SelectedRecord
                                        Hash Left Outer Join Light
                                          condition: sub.__qdb_outer_ref__0_id=o.id
                                          filter: true
                                            Async JIT Filter workers: 1
                                              filter: status='ACTIVE'
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: orders
                                            Hash
                                                GroupBy vectorized: false
                                                  keys: [__qdb_outer_ref__0_id]
                                                  values: [count(*)]
                                                    Filter filter: (trades.order_id>=__qdb_outer_ref__0.__qdb_outer_ref__0_id and trades.order_id<__qdb_outer_ref__0.__qdb_outer_ref__0_id+1)
                                                        Cross Join
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: trades
                                                            Async JIT Group By workers: 1
                                                              keys: [__qdb_outer_ref__0_id]
                                                              filter: status='ACTIVE'
                                                                PageFrame
                                                                    Row forward scan
                                                                    Frame forward scan on: orders
                            """,
                    """
                            SELECT o.id, sub.cnt
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT count(*) AS cnt
                                FROM trades
                                WHERE order_id >= o.id AND order_id < o.id + 1
                            ) sub ON true
                            WHERE status = 'ACTIVE'
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T98c: WHERE with function expression — push down abs(o.id) > 1
    @Test
    public void testT98cOuterWhereWithFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z'),
                    (3, 40.0, '2024-01-01T02:10:00.000000Z')
                    """);

            // Function expression: abs(o.id) > 1 filters orders 2 and 3
            assertQueryAndPlan(
                    """
                            id\tcnt
                            2\t1
                            3\t1
                            """,
                    """
                            Encode sort
                              keys: [id]
                                VirtualRecord
                                  functions: [id,coalesce(cnt,0)]
                                    SelectedRecord
                                        Hash Left Outer Join Light
                                          condition: sub.__qdb_outer_ref__0_id=o.id
                                          filter: true
                                            Async Filter workers: 1
                                              filter: 1<abs(id)
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: orders
                                            Hash
                                                GroupBy vectorized: false
                                                  keys: [__qdb_outer_ref__0_id]
                                                  values: [count(*)]
                                                    Filter filter: (trades.order_id>=__qdb_outer_ref__0.__qdb_outer_ref__0_id and trades.order_id<__qdb_outer_ref__0.__qdb_outer_ref__0_id+1)
                                                        Cross Join
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: trades
                                                            Async Group By workers: 1
                                                              keys: [__qdb_outer_ref__0_id]
                                                              filter: 1<abs(id)
                                                                PageFrame
                                                                    Row forward scan
                                                                    Frame forward scan on: orders
                            """,
                    """
                            SELECT o.id, sub.cnt
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT count(*) AS cnt
                                FROM trades
                                WHERE order_id >= o.id AND order_id < o.id + 1
                            ) sub ON true
                            WHERE abs(o.id) > 1
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T98d: multi-source lateral + qualified WHERE on one source
    @Test
    public void testT98dMultiSourceOuterWhere() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, status SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, category SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t3 (a INT, b SYMBOL, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 'ACTIVE', '2024-01-01T00:00:00.000000Z'),
                    (2, 'CLOSED', '2024-01-01T01:00:00.000000Z'),
                    (3, 'ACTIVE', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 'X', '2024-01-01T00:00:00.000000Z'),
                    (2, 'Y', '2024-01-01T01:00:00.000000Z'),
                    (3, 'X', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t3 VALUES
                    (1, 'X', 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 'X', 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 'Y', 30, '2024-01-01T01:10:00.000000Z'),
                    (3, 'X', 40, '2024-01-01T02:10:00.000000Z')
                    """);

            // Correlates with both t1 and t2; WHERE filters only t1
            assertQueryAndPlan(
                    """
                            id\tcategory\tcnt
                            1\tX\t2
                            3\tX\t1
                            """,
                    """
                            Encode sort
                              keys: [id]
                                SelectedRecord
                                    Hash Join Light
                                      condition: sub.__qdb_outer_ref__0_category=t2.category and sub.__qdb_outer_ref__0_id=t1.id
                                      symbolKeyJoin: true
                                        Hash Join Light
                                          condition: t2.t1_id=t1.id
                                            Async JIT Filter workers: 1
                                              filter: status='ACTIVE'
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: t1
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: t2
                                        Hash
                                            GroupBy vectorized: false
                                              keys: [__qdb_outer_ref__0_category,__qdb_outer_ref__0_id]
                                              values: [count(*)]
                                                Filter filter: (t3.a>=__qdb_outer_ref__0.__qdb_outer_ref__0_id and t3.a<__qdb_outer_ref__0.__qdb_outer_ref__0_id+1)
                                                    Hash Join Light
                                                      condition: __qdb_outer_ref__0_category=t3.b
                                                      symbolKeyJoin: true
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: t3
                                                        Hash
                                                            GroupBy vectorized: false
                                                              keys: [__qdb_outer_ref__0_category,__qdb_outer_ref__0_id]
                                                                SelectedRecord
                                                                    Hash Join Light
                                                                      condition: t2.t1_id=t1.id
                                                                        Async JIT Filter workers: 1
                                                                          filter: status='ACTIVE'
                                                                            PageFrame
                                                                                Row forward scan
                                                                                Frame forward scan on: t1
                                                                        Hash
                                                                            PageFrame
                                                                                Row forward scan
                                                                                Frame forward scan on: t2
                            """,
                    """
                            SELECT t1.id, t2.category, sub.cnt
                            FROM t1
                            JOIN t2 ON t1.id = t2.t1_id
                            JOIN LATERAL (
                                SELECT count(*) AS cnt
                                FROM t3
                                WHERE t3.a >= t1.id AND t3.a < t1.id + 1
                                  AND t3.b = t2.category
                            ) sub ON true
                            WHERE t1.status = 'ACTIVE'
                            ORDER BY t1.id
                            """,
                    null, true, false
            );
        });
    }

    // T98e: WHERE predicate references both outer sources — must NOT be pushed
    // to any single base (predicate stays only in outer WHERE)
    @Test
    public void testT98eWhereAcrossMultipleSources() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t3 (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 10, '2024-01-01T00:00:00.000000Z'),
                    (2, 20, '2024-01-01T01:00:00.000000Z'),
                    (3, 30, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 5, '2024-01-01T00:00:00.000000Z'),
                    (2, 25, '2024-01-01T01:00:00.000000Z'),
                    (3, 15, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t3 VALUES
                    (1, 5, '2024-01-01T00:10:00.000000Z'),
                    (2, 25, '2024-01-01T01:10:00.000000Z'),
                    (3, 15, '2024-01-01T02:10:00.000000Z')
                    """);

            // WHERE t1.val > t2.val spans both sources — cannot be pushed to either base
            assertQueryAndPlan(
                    """
                            id\tt2_val\tcnt
                            1\t5\t1
                            3\t15\t1
                            """,
                    """
                            Encode sort
                              keys: [id]
                                SelectedRecord
                                    Hash Join Light
                                      condition: sub.__qdb_outer_ref__0_val=t2.val and sub.__qdb_outer_ref__0_id=t1.id
                                        Filter filter: t2.val<t1.val
                                            Hash Join Light
                                              condition: t2.t1_id=t1.id
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: t1
                                                Hash
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: t2
                                        Hash
                                            GroupBy vectorized: false
                                              keys: [__qdb_outer_ref__0_val,__qdb_outer_ref__0_id]
                                              values: [count(*)]
                                                Filter filter: (t3.a>=__qdb_outer_ref__0.__qdb_outer_ref__0_id and t3.a<__qdb_outer_ref__0.__qdb_outer_ref__0_id+1)
                                                    Hash Join Light
                                                      condition: __qdb_outer_ref__0_val=t3.b
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: t3
                                                        Hash
                                                            GroupBy vectorized: false
                                                              keys: [__qdb_outer_ref__0_val,__qdb_outer_ref__0_id]
                                                                SelectedRecord
                                                                    Hash Join Light
                                                                      condition: t2.t1_id=t1.id
                                                                        PageFrame
                                                                            Row forward scan
                                                                            Frame forward scan on: t1
                                                                        Hash
                                                                            PageFrame
                                                                                Row forward scan
                                                                                Frame forward scan on: t2
                            """,
                    """
                            SELECT t1.id, t2.val AS t2_val, sub.cnt
                            FROM t1
                            JOIN t2 ON t1.id = t2.t1_id
                            JOIN LATERAL (
                                SELECT count(*) AS cnt
                                FROM t3
                                WHERE t3.a >= t1.id AND t3.a < t1.id + 1
                                  AND t3.b = t2.val
                            ) sub ON true
                            WHERE t1.val > t2.val
                            ORDER BY t1.id
                            """,
                    null, true, false
            );
        });
    }

    // T99: BETWEEN with outer refs — common time-range lateral pattern
    @Test
    public void testT99BetweenWithOuterRefs() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE windows (id INT, start_ts TIMESTAMP, end_ts TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE events (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO windows VALUES
                    (1, '2024-01-01T00:00:00.000000Z', '2024-01-01T01:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T02:00:00.000000Z', '2024-01-01T03:00:00.000000Z', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO events VALUES
                    (10, '2024-01-01T00:30:00.000000Z'),
                    (20, '2024-01-01T00:45:00.000000Z'),
                    (30, '2024-01-01T01:30:00.000000Z'),
                    (40, '2024-01-01T02:30:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\t10
                            1\t20
                            2\t40
                            """,
                    """
                            SELECT w.id, sub.val
                            FROM windows w
                            JOIN LATERAL (
                                SELECT val FROM events
                                WHERE ts >= w.start_ts AND ts < w.end_ts
                            ) sub
                            ORDER BY w.id, sub.val
                            """,
                    null, true, true
            );
        });
    }

    // T99: correlated ON on join branch (ji > 0, INNER JOIN)
    // The join branch (t3) has correlated ON: t3.order_id = o.id
    // pushDownOuterRefsForJoinBranch moves it to t3's WHERE
    @Test
    public void testT99CorrelatedOnJoinBranch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE adjustments (order_id INT, adj DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO adjustments VALUES
                    (1, 1.5, '2024-01-01T00:00:00.000000Z'),
                    (2, 2.0, '2024-01-01T01:00:00.000000Z')
                    """);

            // Main chain (trades): correlated WHERE order_id = o.id
            // Join branch (adjustments): correlated ON order_id = o.id
            // Both branches get independent __qdb_outer_ref__ clones
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tadj
                            1\t10.0\t1.5
                            1\t20.0\t1.5
                            2\t30.0\t2.0
                            """,
                    """
                            SELECT o.id, sub.qty, sub.adj
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, a.adj
                                FROM trades t
                                JOIN adjustments a ON a.order_id = o.id
                                WHERE t.order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.qty
                            """,
                    null, true, true
            );
        });
    }

    // T99b: LEFT JOIN branch with correlated ON inside lateral
    // The LEFT JOIN semantics must be preserved: unmatched trades rows should
    // produce NULL for the adjustment columns
    @Test
    public void testT99bLeftJoinBranchCorrelatedOn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE adjustments (order_id INT, adj DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            // Only order 1 has an adjustment
            execute("""
                    INSERT INTO adjustments VALUES
                    (1, 1.5, '2024-01-01T00:00:00.000000Z')
                    """);

            // LEFT JOIN: order 2 trades should still appear with NULL adj
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tadj
                            1\t10.0\t1.5
                            1\t20.0\t1.5
                            2\t30.0\tnull
                            """,
                    """
                            SELECT o.id, sub.qty, sub.adj
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, a.adj
                                FROM trades t
                                LEFT JOIN adjustments a ON a.order_id = o.id
                                WHERE t.order_id = o.id
                            ) sub
                            ORDER BY o.id, sub.qty
                            """,
                    null, true, true
            );
        });
    }

    // T99c: multiple correlated join branches inside lateral
    // Both join branches (adjustments and discounts) have correlated ON criteria
    @Test
    public void testT99cMultipleCorrelatedJoinBranches() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE adjustments (order_id INT, adj DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE discounts (order_id INT, disc DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO adjustments VALUES
                    (1, 1.5, '2024-01-01T00:00:00.000000Z'),
                    (2, 2.0, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO discounts VALUES
                    (1, 0.9, '2024-01-01T00:00:00.000000Z'),
                    (2, 0.8, '2024-01-01T01:00:00.000000Z')
                    """);

            // Three branches: trades (WHERE), adjustments (ON), discounts (ON)
            // Each correlated branch gets its own cloned __qdb_outer_ref__
            assertQueryNoLeakCheck(
                    """
                            id\tqty\tadj\tdisc
                            1\t10.0\t1.5\t0.9
                            2\t30.0\t2.0\t0.8
                            """,
                    """
                            SELECT o.id, sub.qty, sub.adj, sub.disc
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t.qty, a.adj, d.disc
                                FROM trades t
                                JOIN adjustments a ON a.order_id = o.id
                                JOIN discounts d ON d.order_id = o.id
                                WHERE t.order_id = o.id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, true
            );
        });
    }

    private void createOrdersAndTrades() throws Exception {
        execute("CREATE TABLE orders (id INT, customer STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO orders VALUES
                (1, 'Alice', '2024-01-01T00:00:00.000000Z'),
                (2, 'Bob', '2024-01-01T01:00:00.000000Z'),
                (3, 'Charlie', '2024-01-01T02:00:00.000000Z')
                """);
        execute("""
                INSERT INTO trades VALUES
                (1, 1, 10.0, 100.0, '2024-01-01T00:30:00.000000Z'),
                (2, 1, 20.0, 200.0, '2024-01-01T00:45:00.000000Z'),
                (3, 2, 30.0, 300.0, '2024-01-01T01:30:00.000000Z'),
                (4, 3, 40.0, 400.0, '2024-01-01T02:30:00.000000Z'),
                (5, 3, 50.0, 500.0, '2024-01-01T02:45:00.000000Z')
                """);
    }
}
