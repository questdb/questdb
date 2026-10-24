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

import io.questdb.PropertyKey;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlOptimiser;
import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestTimestampCounterFactory;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.QueryModelWrapper;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVarTuple;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

public class LateralJoinTest extends AbstractCairoTest {
    // over the tables of createCorrelatedRightJoinTables(), the trade that refund 100 matches for
    // o.k = 1 on t.x = r.k AND r.k = o.k
    private static final String TRADE_10 = "1\t10\t100\n";
    // over the tables of createCorrelatedRightJoinTables(), the refunds that a RIGHT join on
    // t.x = r.k AND r.k = o.k, or after a join with xs on o.k, NULL-extends per outer row
    private static final String UNMATCHED_REFUNDS = """
            1\tnull\t101
            2\tnull\t100
            2\tnull\t101
            """;
    // TRADE_10 and UNMATCHED_REFUNDS, in the order of ORDER BY 1, 2, 3
    private static final String TRADE_10_AND_UNMATCHED_REFUNDS = """
            1\tnull\t101
            1\t10\t100
            2\tnull\t100
            2\tnull\t101
            """;

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

            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    CROSS JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            1\t10.0
                            2\t20.0
                            """);
        });
    }

    @Test
    public void testCrossLateralPlainScalarAggregateRuntimeLimitDropsRows() throws Exception {
        assertMemoryLeak(() -> assertPlainScalarAggregateRuntimeLimit(
                """
                        SELECT t1.k, l.m
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT max(v) AS m
                            FROM t2
                            WHERE t2.k = t1.k
                            LIMIT :lim
                        ) l
                        ORDER BY t1.k
                        """,
                "k\tm\n1\t20\n2\t30\n",
                "k\tm\n"
        ));
    }

    @Test
    public void testCrossLateralScalarCountWrapperFilterRejectsRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (a INT)");
            execute("INSERT INTO o VALUES (1), (2)");
            execute("CREATE TABLE t (x INT)");
            execute("INSERT INTO t VALUES (1)");

            String sql = """
                    SELECT o.a, l.cnt
                    FROM o
                    CROSS JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t
                            WHERE t.x = o.a
                        ) counted
                        WHERE cnt > 1
                    ) l
                    ORDER BY o.a
                    """;

            assertQuery(sql)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            """);

            execute("INSERT INTO t VALUES (1)");
            assertQuery(sql)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t2
                            """);

            assertQuery("""
                    SELECT o.a, l.cnt
                    FROM o
                    CROSS JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t
                            WHERE t.x = o.a
                        ) counted
                        WHERE cnt >= 0
                    ) l
                    ORDER BY o.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t2
                            2\t0
                            """);
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

            assertQuery("""
                    SELECT t1.a, x.total
                    FROM t1
                    JOIN LATERAL (
                        SELECT sum(val) AS total
                        FROM (SELECT val, t1_id FROM t2 WHERE t2.t1_id = t1.a)
                        WHERE t1_id = t1.a
                        GROUP BY t1.a
                    ) x
                    ORDER BY t1.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\ttotal
                            1\t13
                            2\t50
                            """);
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

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tresult
                            1\t110
                            2\t230
                            """);
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

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tb
                            1\t100
                            1\t100
                            2\t200
                            """);
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

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tresult
                            1\t110
                            2\t230
                            """);
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

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tresult
                            1\t103
                            1\t110
                            2\t230
                            """);
        });
    }

    @Test
    public void testGeneratedColumnWildcardMetadataLifecycle() {
        QueryColumn column = new QueryColumn().of("generated", null);
        Assert.assertFalse(column.isGenerated());

        column.setGenerated(true);
        column.of("renamed", null);
        Assert.assertTrue(column.isGenerated());

        column.clear();
        Assert.assertFalse(column.isGenerated());
        Assert.assertTrue(column.isIncludeIntoWildcard());
    }

    @Test
    public void testInnerLateralPlainScalarAggregateRuntimeLimitDropsRows() throws Exception {
        assertMemoryLeak(() -> assertPlainScalarAggregateRuntimeLimit(
                """
                        SELECT t1.k, l.s
                        FROM t1
                        JOIN LATERAL (
                            SELECT sum(v) AS s
                            FROM t2
                            WHERE t2.k = t1.k
                            LIMIT :lim
                        ) l ON true
                        ORDER BY t1.k
                        """,
                "k\ts\n1\t30\n2\t30\n",
                "k\ts\n"
        ));
    }

    @Test
    public void testInnerLateralScalarAggregateJoinBranchOnRejectsRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, 10)");

            assertQuery("""
                    SELECT t0.a, l1.k, l1.m
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.m
                        FROM t1
                        JOIN LATERAL (
                            SELECT max(v) AS m
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2 ON l2.m > 0
                        WHERE t1.k = t0.a
                    ) l1 ON true
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tm
                            1\t1\t10
                            2\tnull\tnull
                            """);
        });
    }

    @Test
    public void testInnerLateralScalarCountDistinctJoinBranchOnRejectsRowNonPerSide() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.k, l1.v
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.v
                        FROM t1
                        JOIN LATERAL (
                            SELECT count_distinct(t2.x) AS v
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2 ON l2.v > 0
                        WHERE t1.k = t0.a
                    ) l1 ON true
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tv
                            1\t1\t1
                            2\tnull\tnull
                            """);
        });
    }

    @Test
    public void testInnerLateralScalarCountDistinctJoinBranchOnRejectsRowPerSide() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.k, l1.v
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.v
                        FROM t1
                        JOIN LATERAL (
                            SELECT count_distinct(t2.x) AS v
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2 ON l2.v > 0
                    ) l1 ON true
                    ORDER BY t0.a, l1.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tv
                            1\t1\t1
                            1\t2\t1
                            2\tnull\tnull
                            """);
        });
    }

    @Test
    public void testInnerLateralScalarCountJoinBranchOnRejectsRowKeepsBareCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.k, l1.v
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.v
                        FROM t1
                        JOIN LATERAL (
                            SELECT count(*) AS v
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2 ON l2.v > 0
                        WHERE t1.k = t0.a
                    ) l1 ON true
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tv
                            1\t1\t1
                            2\t2\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.k, l1.v
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.v
                        FROM t1
                        JOIN LATERAL (
                            SELECT c AS v
                            FROM (
                                SELECT count(*) AS c
                                FROM t2
                                WHERE t2.x = t0.a
                            ) counted
                        ) l2 ON l2.v > 0
                        WHERE t1.k = t0.a
                    ) l1 ON true
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tv
                            1\t1\t1
                            2\t2\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.k, l1.v
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.v
                        FROM t1
                        JOIN LATERAL (
                            SELECT count(*) AS v
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2 ON l2.v > 0
                    ) l1 ON true
                    ORDER BY t0.a, l1.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tv
                            1\t1\t1
                            1\t2\t1
                            2\t1\t0
                            2\t2\t0
                            """);
        });
    }

    @Test
    public void testInnerLateralScalarCountJoinBranchOnRejectsRowNonPerSide() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.k, l1.v
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.v
                        FROM t1
                        JOIN LATERAL (
                            SELECT count(*) + 2 AS v
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2 ON l2.v > 2
                        WHERE t1.k = t0.a
                    ) l1 ON true
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tv
                            1\t1\t3
                            2\tnull\tnull
                            """);
        });
    }

    @Test
    public void testInnerLateralScalarCountJoinBranchOnRejectsRowPerSide() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.k, l1.v
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.v
                        FROM t1
                        JOIN LATERAL (
                            SELECT count(*) + 2 AS v
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2 ON l2.v > 2
                    ) l1 ON true
                    ORDER BY t0.a, l1.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tv
                            1\t1\t3
                            1\t2\t3
                            2\tnull\tnull
                            """);
        });
    }

    @Test
    public void testInnerLateralScalarCountJoinBranchTrivialOnDoesNotRetainFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.k, l1.v
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.v
                        FROM t1
                        JOIN LATERAL (
                            SELECT count(*) AS v
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2 ON true
                        WHERE t1.k = t0.a
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .withPlanContaining(
                            "Hash Left Outer Join Light",
                            "condition: l2.__qdb_outer_ref__1_a=t1.k"
                    )
                    .withPlanNotContaining("filter: true")
                    .returns("""
                            a\tk\tv
                            1\t1\t1
                            2\t2\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.k, l1.v
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.v
                        FROM t1
                        JOIN LATERAL (
                            SELECT count(*) AS v
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2 ON true
                    ) l1
                    ORDER BY t0.a, l1.k
                    """)
                    .noLeakCheck()
                    .withPlanContaining(
                            "Hash Left Outer Join Light",
                            "condition: l2.__qdb_outer_ref__2_a=__qdb_count_driver__1.__qdb_count_driver__1_a"
                    )
                    .withPlanNotContaining("filter: true")
                    .returns("""
                            a\tk\tv
                            1\t1\t1
                            1\t2\t1
                            2\t1\t0
                            2\t2\t0
                            """);
        });
    }

    @Test
    public void testInnerLateralScalarCountOnRejectsRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t1.k, l.c
                    FROM t1
                    JOIN LATERAL (
                        SELECT count(*) AS c
                        FROM t2
                        WHERE t2.k = t1.k
                    ) l ON l.c > 0
                    ORDER BY t1.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\tc
                            1\t2
                            """);
        });
    }

    @Test
    public void testInnerLateralScalarCountRuntimeLimitDropsRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            String crossSql = "SELECT t1.k, l.c FROM t1 CROSS JOIN LATERAL "
                    + "(SELECT count(*) AS c FROM t2 WHERE t2.k = t1.k LIMIT :lim) l ORDER BY t1.k";
            String innerSql = "SELECT t1.k, l.c FROM t1 JOIN LATERAL "
                    + "(SELECT count(*) AS c FROM t2 WHERE t2.k = t1.k LIMIT :lim) l ON true ORDER BY t1.k";

            bindVariableService.clear();
            bindVariableService.setLong("lim", 1);
            assertQuery(crossSql).noLeakCheck().returns("""
                    k\tc
                    1\t2
                    2\t0
                    """);
            assertQuery(innerSql).noLeakCheck().returns("""
                    k\tc
                    1\t2
                    2\t0
                    """);

            bindVariableService.setLong("lim", 0);
            assertQuery(crossSql).noLeakCheck().returns("""
                    k\tc
                    """);
            assertQuery(innerSql).noLeakCheck().returns("""
                    k\tc
                    """);
        });
    }

    @Test
    public void testInnerLateralScalarCountWrapperFilterRejectsRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (a INT)");
            execute("INSERT INTO o VALUES (1), (2)");
            execute("CREATE TABLE t (x INT)");
            execute("INSERT INTO t VALUES (1)");

            String sql = """
                    SELECT o.a, l.cnt
                    FROM o
                    JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t
                            WHERE t.x = o.a
                        ) counted
                        WHERE cnt > 1
                    ) l ON true
                    ORDER BY o.a
                    """;

            assertQuery(sql)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            """);

            execute("INSERT INTO t VALUES (1)");
            assertQuery(sql)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t2
                            """);
        });
    }

    @Test
    public void testLateralAliasCollisionCarrierNotLeakedWhenNotEliminable() throws Exception {
        // Correlation through an expression (t0.a + 0) cannot be eliminated, so the
        // hidden __qdb_outer_ref__ carrier column survives in the decorrelated join
        // model. SELECT * and l1.* must not expose it, and the scalar count must still
        // coalesce to 0 for non-matching outer rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, 42)");

            assertQuery("""
                    SELECT *
                    FROM t0
                    LEFT JOIN LATERAL (SELECT count(*) cnt FROM t2 WHERE t2.x = t0.a + 0) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tb\tcnt
                            1\t10\t1
                            2\t20\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (SELECT count(*) cnt FROM t2 WHERE t2.x = t0.a + 0) l1
                    ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);
        });
    }

    @Test
    public void testLateralAliasCollisionCountAliasedAsCorrelationColumn() throws Exception {
        // The user aliases the scalar count with the correlation column name (x). The
        // decorrelated sub-query carries its own hidden x-derived key column; neither
        // wildcard shape may expose it, and the count keeps 0-for-no-match semantics.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, 42)");

            assertQuery("""
                    SELECT *
                    FROM t0
                    LEFT JOIN LATERAL (SELECT count(*) AS x FROM t2 WHERE t2.x = t0.a) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tb\tx
                            1\t10\t1
                            2\t20\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (SELECT count(*) AS x FROM t2 WHERE t2.x = t0.a) l1
                    ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tx
                            1\t1
                            2\t0
                            """);
        });
    }

    @Test
    public void testLateralAliasCollisionFunctionStealsCorrelationColumn() throws Exception {
        // Inside the lateral body a function takes the correlation column's name that
        // GROUP BY also refers to. Grouped (non-scalar) count keeps SQL
        // NULL semantics for unmatched outer rows - a contrast to the scalar-count
        // coalesce - and no helper columns may leak into either wildcard.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, 42)");

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (SELECT abs(x) x, count(*) cnt FROM t2 WHERE t2.x = t0.a GROUP BY x) l1
                    ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tx\tcnt
                            1\t1\t1
                            2\tnull\tnull
                            """);

            assertQuery("""
                    SELECT *
                    FROM t0
                    LEFT JOIN LATERAL (SELECT abs(x) x, count(*) cnt FROM t2 WHERE t2.x = t0.a GROUP BY x) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tb\tx\tcnt
                            1\t10\t1\t1
                            2\t20\tnull\tnull
                            """);
        });
    }

    // A bare (unqualified) reference to the lateral scalar-count column keeps its
    // coalesce-to-zero compensation even when an outer projection alias steals the
    // column name: the parser dedupes the bare ref (cnt -> cnt1), and the scalar-count
    // marking must follow the join column the data comes from, not the same-named
    // projection alias. See also the qualified-reference twin
    // (testLateralAliasCollisionScalarCountVsOuterProjection) and the collision-free
    // baseline (testLateralAliasCollisionScalarCountBareReferenceBaseline).
    @Test
    public void testLateralAliasCollisionScalarCountBareRefStolenAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, 42)");

            assertQuery("""
                    SELECT abs(t0.b) cnt, cnt
                    FROM t0
                    LEFT JOIN LATERAL (SELECT count(*) cnt FROM t2 WHERE t2.x = t0.a) l1
                    ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            cnt\tcnt1
                            10\t1
                            20\t0
                            """);

            // same shape with a non-eliminable correlation
            assertQuery("""
                    SELECT abs(t0.b) cnt, cnt
                    FROM t0
                    LEFT JOIN LATERAL (SELECT count(*) cnt FROM t2 WHERE t2.x = t0.a + 0) l1
                    ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            cnt\tcnt1
                            10\t1
                            20\t0
                            """);

            // bare reference inside an expression, same stolen alias: the coalesced
            // count feeds the arithmetic, so the unmatched row yields 0, not NULL
            assertQuery("""
                    SELECT abs(t0.b) cnt, cnt + 0 c2
                    FROM t0
                    LEFT JOIN LATERAL (SELECT count(*) cnt FROM t2 WHERE t2.x = t0.a) l1
                    ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            cnt\tc2
                            10\t1
                            20\t0
                            """);
        });
    }

    @Test
    public void testLateralAliasCollisionScalarCountBareReferenceBaseline() throws Exception {
        // A bare (unqualified) reference to the lateral scalar-count column must carry
        // the coalesce-to-zero semantics the same way a qualified reference does.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, 42)");

            assertQuery("""
                    SELECT cnt, abs(t0.b) cnt2
                    FROM t0
                    LEFT JOIN LATERAL (SELECT count(*) cnt FROM t2 WHERE t2.x = t0.a) l1
                    ORDER BY cnt
                    """)
                    .noLeakCheck()
                    .returns("""
                            cnt\tcnt2
                            0\t20
                            1\t10
                            """);
        });
    }

    @Test
    public void testLateralAliasCollisionScalarCountVsOuterProjection() throws Exception {
        // The outer projection declares a function column with the same alias as the
        // lateral scalar-count column. The count reference is renamed (cnt -> cnt1) to
        // stay unique, and must keep scalar-count semantics: 0 for a non-matching outer
        // row, never NULL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, 42)");

            assertQuery("""
                    SELECT abs(t0.b) cnt, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (SELECT count(*) cnt FROM t2 WHERE t2.x = t0.a) l1
                    ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            cnt\tcnt1
                            10\t1
                            20\t0
                            """);

            // same collision with a non-eliminable correlation (expression forces the
            // decorrelated outer-ref carrier to survive into the execution plan)
            assertQuery("""
                    SELECT abs(t0.b) cnt, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (SELECT count(*) cnt FROM t2 WHERE t2.x = t0.a + 0) l1
                    ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            cnt\tcnt1
                            10\t1
                            20\t0
                            """);
        });
    }

    @Test
    public void testLateralAliasCollisionUserColumnOccupiesCarrierName() throws Exception {
        // The user declares a column whose alias matches the generated correlation
        // carrier name. The rewriter must keep the two apart: the user column stays
        // visible under its name, the internal carrier stays hidden, and scalar count
        // still coalesces to 0. The compensation rebuilds the row the aggregate body
        // yields for an empty input, so the deterministic sibling constant
        // materializes alongside the zero count for the unmatched outer row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, 42)");

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (SELECT count(*) cnt, 1 __qdb_outer_ref__0_a FROM t2 WHERE t2.x = t0.a) l1
                    ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt\t__qdb_outer_ref__0_a
                            1\t1\t1
                            2\t0\t1
                            """);
        });
    }

    // A chain LIMIT on a per-side-push-eligible NON-count body must apply per
    // outer row, not globally over the decorrelated result.
    @Test
    public void testLateralChainLimitPerSideShapePerOuterRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t0 (k INT, x INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (1, 11), (1, 12), (2, 20)");
            execute("CREATE TABLE t3 (k INT)");
            execute("INSERT INTO t3 VALUES (1), (2), (3)");

            // a global LIMIT 2 would keep two rows TOTAL; per outer row it keeps
            // two rows for k=1, one for k=2, and LEFT preserves k=3
            assertQuery("SELECT t1.k, l.x FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT t0.x FROM t0 "
                    + " JOIN (SELECT k FROM t3 WHERE t3.k = t1.k) s ON s.k = t0.k "
                    + " ORDER BY t0.x LIMIT 2) l ON true ORDER BY t1.k, l.x")
                    .noLeakCheck()
                    .returns("""
                            k\tx
                            1\t10
                            1\t11
                            2\t20
                            3\tnull
                            """);

            // INNER lateral: same per-outer-row limiting, no row preservation
            assertQuery("SELECT t1.k, l.x FROM t1 JOIN LATERAL "
                    + "(SELECT t0.x FROM t0 "
                    + " JOIN (SELECT k FROM t3 WHERE t3.k = t1.k) s ON s.k = t0.k "
                    + " ORDER BY t0.x LIMIT 2) l ON true ORDER BY t1.k, l.x")
                    .noLeakCheck()
                    .returns("""
                            k\tx
                            1\t10
                            1\t11
                            2\t20
                            """);
        });
    }

    // Chain LIMITs on the per-side-eligible shape follow the general-path
    // contracts: provably negative values and outer-column references over a
    // scalar count are rejected, for LEFT and INNER laterals alike.
    @Test
    public void testLateralChainLimitPerSideShapeRejections() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT, n INT)");
            execute("INSERT INTO t1 VALUES (1, 1), (2, 1), (3, 1)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");
            execute("CREATE TABLE t3 (k INT)");
            execute("INSERT INTO t3 VALUES (1), (2), (3)");

            String negativeLeft = "SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 "
                    + " JOIN (SELECT k FROM t3 WHERE t3.k = t1.k) s ON s.k = t2.k "
                    + " LIMIT -1) l ON true";
            assertException(negativeLeft, negativeLeft.indexOf("-1"),
                    "negative LIMIT is not supported in a correlated lateral sub-query");

            String negativeInner = "SELECT t1.k, l.x FROM t1 JOIN LATERAL "
                    + "(SELECT t2.k x FROM t2 "
                    + " JOIN (SELECT k FROM t3 WHERE t3.k = t1.k) s ON s.k = t2.k "
                    + " LIMIT -1) l ON true";
            assertException(negativeInner, negativeInner.indexOf("-1"),
                    "negative LIMIT is not supported in a correlated lateral sub-query");

            String outerColumn = "SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 "
                    + " JOIN (SELECT k FROM t3 WHERE t3.k = t1.k) s ON s.k = t2.k "
                    + " LIMIT t1.n) l ON true";
            assertException(outerColumn, outerColumn.indexOf("t1.n"),
                    "LIMIT referencing an outer column is not supported over a scalar count");
        });
    }

    // The correlated WHERE conjunct p.k = o.k reads the slave of the ASOF join. With the
    // non-equality t.v <= o.v and the aggregate, the rewriter keeps the outer-ref join, which it
    // inserts before the ASOF join and whose ON holds p.k = o.k. That conjunct is not a forward
    // reference: it must filter after the ASOF join.
    @Test
    public void testLateralCorrelatedEqualityOnAsOfJoinSlaveWithAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createOuterRefJoinSlaveTables();
            assertQuery("SELECT o.ok, s.c FROM o JOIN LATERAL (SELECT count(*) c FROM t ASOF JOIN p ON (id) WHERE p.k = o.k AND t.v <= o.v) s ORDER BY 1")
                    .noLeakCheck()
                    .returns("""
                            ok\tc
                            1\t1
                            2\t1
                            3\t0
                            """);
        });
    }

    // Same as testLateralCorrelatedEqualityOnAsOfJoinSlaveWithAggregate, for a LEFT join slave.
    @Test
    public void testLateralCorrelatedEqualityOnLeftJoinSlaveWithAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createOuterRefJoinSlaveTables();
            assertQuery("SELECT o.ok, s.c FROM o JOIN LATERAL (SELECT count(*) c FROM t LEFT JOIN p ON t.id = p.id WHERE p.k = o.k AND t.v <= o.v) s ORDER BY 1")
                    .noLeakCheck()
                    .returns("""
                            ok\tc
                            1\t1
                            2\t1
                            3\t1
                            """);
        });
    }

    // Same as testLateralCorrelatedEqualityOnAsOfJoinSlaveWithAggregate, for an LT join slave.
    @Test
    public void testLateralCorrelatedEqualityOnLtJoinSlaveWithAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createOuterRefJoinSlaveTables();
            assertQuery("SELECT o.ok, s.c FROM o JOIN LATERAL (SELECT count(*) c FROM t LT JOIN p ON (id) WHERE p.k = o.k AND t.v <= o.v) s ORDER BY 1")
                    .noLeakCheck()
                    .returns("""
                            ok\tc
                            1\t1
                            2\t1
                            3\t0
                            """);
        });
    }

    // The ON clause of the FULL join reads the outer row, and both sides of the join keep the rows
    // it does not match. Per outer row, o.k = 1 matches trade 10 with refund 100 and keeps refund
    // 101 unmatched, and o.k = 2 keeps trade 10 and refunds 100 and 101 unmatched.
    @Test
    public void testLateralCorrelatedFullJoinOn() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t FULL JOIN refunds r ON r.k = o.k AND t.x = r.k AND t.id > 0) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            1\t10\t100
                            2\tnull\t100
                            2\tnull\t101
                            2\t10\tnull
                            """);
        });
    }

    // WHERE a.id > 0 drops the rows in which the FULL join NULL-extends a. Per outer row, a 10 matches
    // b 20 for o.k = 1, and a 11 matches b 21 for o.k = 2. The other a row stays with a NULL bid, and
    // the b row that matches no a row has a NULL aid, which the filter drops.
    @Test
    public void testLateralCorrelatedFullJoinOnWithMasterFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, x INT)");
            execute("CREATE TABLE a (id INT, k INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1, 1), (2, 2, 2)");
            execute("INSERT INTO a VALUES (10, 1, 1), (11, 2, 2)");
            execute("INSERT INTO b VALUES (20, 1, 1), (21, 2, 3)");
            for (String on : new String[]{"a.x = b.k AND b.k = o.k", "b.k = o.k AND a.x = b.k"}) {
                final String body = "SELECT a.id aid, b.id bid FROM a FULL JOIN b ON " + on + " WHERE a.id > 0";
                assertQuery("SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (" + body + ") l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                id\taid\tbid
                                1\t10\t20
                                1\t11\tnull
                                2\t10\tnull
                                2\t11\t21
                                """);
                assertQuery("SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (" + body + " AND b.id > 0) l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                id\taid\tbid
                                1\t10\t20
                                2\t11\t21
                                """);
            }
        });
    }

    // Per outer row, the FULL join emits the b rows of s that match no a row with a NULL a.x, and
    // WHERE a.x = o.x keeps them only for a NULL o.x: 2 null 22. Each b row carries the outer row of
    // its own copy of s, so the b rows of o.x = 1 do not reach the NULL o.x. For o.x = 1, a 10
    // matches b 21.
    @Test
    public void testLateralCorrelatedFullJoinSubQueryWhereKey() throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM a FULL JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE a.x = o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            id\taid\tsid
                            1\t10\t21
                            2\tnull\t22
                            """);
        });
    }

    // A FULL join to the correlated sub-query, keyed on a TIMESTAMP_NS or string column against an
    // outer column of another type, with a condition next to the WHERE key that reads the outer
    // column. Per outer row, a row 10 matches b row 20 for o row 1 and has a y below o.ts + 1 or
    // o.s. The rows that the FULL join NULL-extends have a NULL a.ts or a.s, which equals the outer
    // column of o row 3 only, and NULL < NULL is false, so WHERE drops them.
    @Test
    public void testLateralCorrelatedFullJoinSubQueryWhereKeyRetypedKey() throws Exception {
        assertMemoryLeak(() -> {
            final String rows = """
                    id\taid\tsid
                    1\t10\t20
                    """;
            createRetypedKeyTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM a FULL JOIN (SELECT id, k, ts FROM b WHERE ts = o.ts) s ON a.ts = s.k WHERE a.ts = o.ts AND a.y < o.ts + 1) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns(rows);
            execute("DROP TABLE o");
            execute("DROP TABLE a");
            execute("DROP TABLE b");

            createRetypedStringKeyTables("VARCHAR", "SYMBOL");
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM a FULL JOIN (SELECT id, k, s FROM b WHERE s = o.s) s ON a.s = s.k WHERE a.s = o.s AND a.y < o.s) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns(rows);
        });
    }

    // WHERE a.id > 0 drops the rows in which the FULL join NULL-extends a, per outer row too, so
    // the decorrelated query is exact.
    @Test
    public void testLateralCorrelatedFullJoinSubQueryWhereKeyWithMasterFilter() throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM a FULL JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE a.x = o.x AND a.id > 0) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t21
                            """);
        });
    }

    // The correlated ON of the INNER join before the FULL join joins trade 10 only for o.k = 1, and
    // the FULL join keeps the refunds it does not match per outer row: refund 101 for o.k = 1, and
    // both refunds for o.k = 2.
    @Test
    public void testLateralCorrelatedInnerOnBeforeFullJoin() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t JOIN xs x ON x.k = o.k FULL JOIN refunds r ON r.k = t.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            1\t10\t100
                            2\tnull\t100
                            2\tnull\t101
                            """);
        });
    }

    // IN with non-NULL values drops the rows in which the FULL join NULL-extends trades, per outer
    // row too, so losing the unmatched refunds changes nothing. Trade 12 matches no refund and
    // stays, with a NULL rid, for both outer rows that join xs.
    @Test
    public void testLateralCorrelatedInnerOnBeforeFullJoinWithInFilter() throws Exception {
        assertMemoryLeak(() -> {
            createNullableCorrelatedRightJoinTables();
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t JOIN xs x ON x.k = o.k FULL JOIN refunds r ON r.k = t.x WHERE t.id IN (10, 12)) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            1\t12\tnull
                            3\t10\t100
                            3\t12\tnull
                            """);
        });
    }

    // The sub-query picks trade 20, the only one with id > 10. xs matches order 1 only, so per
    // outer row trades 10 and 20 join refunds 100 and 101 for order 1, and t.id >= 20 keeps
    // trade 20 with refund 101. The RIGHT or FULL join NULL-extends both refunds for order 2,
    // and the filter drops them.
    @Test
    public void testLateralCorrelatedInnerOnBeforeOuterJoinWithScalarSubQueryWhereFilter() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            execute("INSERT INTO trades VALUES (20, 2, 2::timestamp)");
            for (String join : new String[]{"RIGHT", "FULL"}) {
                assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t JOIN xs x ON x.k = o.k " + join + " JOIN refunds r ON r.k = t.x WHERE t.id >= (SELECT id FROM trades WHERE id > 10 LIMIT 1)) l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                id\ttid\trid
                                1\t20\t101
                                """);
            }
        });
    }

    // In the rows that the RIGHT join NULL-extends, the INT id that the sub-query passes through holds
    // NULL, which IN (10, 15) drops, and the computed SHORT id holds 0, which IN (0, 10) keeps.
    @Test
    public void testLateralCorrelatedInnerOnBeforeRightJoinSubQueryWithInFilter() throws Exception {
        assertMemoryLeak(() -> {
            createNullableCorrelatedRightJoinTables();
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM (SELECT id, x FROM trades) t JOIN xs x ON x.k = o.k RIGHT JOIN refunds r ON r.k = t.x WHERE t.id IN (10, 15)) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            1\t15\t102
                            3\t10\t100
                            3\t15\t102
                            """);
            // the SHORT id of trade NULL is 0 too
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM (SELECT id::short id, x FROM trades) t JOIN xs x ON x.k = o.k RIGHT JOIN refunds r ON r.k = t.x WHERE t.id IN (0, 10)) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            id\ttid\trid
                            1\t0\t100
                            1\t0\t101
                            1\t10\t100
                            2\t0\t100
                            2\t0\t101
                            2\t0\t102
                            3\t0\t100
                            3\t0\t101
                            3\t10\t100
                            """);
        });
    }

    // BETWEEN is false for a NULL value, so it drops the refunds that match no trade, per outer row
    // too. NOT BETWEEN keeps them, and trade NULL, which refund 100 matches for o.k = 1 and NULL.
    @Test
    public void testLateralCorrelatedInnerOnBeforeRightJoinWithBetweenFilter() throws Exception {
        assertMemoryLeak(() -> {
            createNullableCorrelatedRightJoinTables();
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t JOIN xs x ON x.k = o.k RIGHT JOIN refunds r ON r.k = t.x WHERE t.id BETWEEN 11 AND 15) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t15\t102
                            3\t15\t102
                            """);
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t JOIN xs x ON x.k = o.k RIGHT JOIN refunds r ON r.k = t.x WHERE t.id NOT BETWEEN 11 AND 15) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t100
                            1\tnull\t101
                            1\t10\t100
                            2\tnull\t100
                            2\tnull\t101
                            2\tnull\t102
                            3\tnull\t100
                            3\tnull\t101
                            3\t10\t100
                            """);
        });
    }

    // WHERE t.id = $1 drops the refunds that match no trade per outer row while $1 is not NULL, as a
    // NULL t.id does not equal it: for $1 = 10, order 1 returns 10 100. For a NULL $1, = keeps those
    // refunds. The query compiles once for all three executions.
    @Test
    public void testLateralCorrelatedInnerOnBeforeRightJoinWithBindVariableEquality() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            final String sql = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t JOIN xs x ON x.k = o.k RIGHT JOIN refunds r ON r.k = t.x WHERE t.id = $1) l ORDER BY 1, 2, 3";
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .assertBinds(nullValueBindCases(10, UNMATCHED_REFUNDS));
        });
    }

    // Against a bind variable, = drops the refunds that match no trade from a computed INT column
    // while the value is not NULL, and keeps them while it is NULL. A computed SHORT column holds 0 in
    // those refunds, so = keeps them while the value is 0.
    @Test
    public void testLateralCorrelatedInnerOnBeforeRightJoinWithComputedColumnBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            final String sql = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM (SELECT id + 0 id, x FROM trades) t JOIN xs x ON x.k = o.k RIGHT JOIN refunds r ON r.k = t.x WHERE t.id = $1) l ORDER BY 1, 2, 3";
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .assertBinds(nullValueBindCases(10, UNMATCHED_REFUNDS));
            bindVariableService.clear();
            bindVariableService.setShort(0, (short) 10);
            final ObjList<BindVarTuple> shortCases = new ObjList<>();
            shortCases.add(BindVarTuple.ok("value", "id\ttid\trid\n" + TRADE_10, bindVariableService -> bindVariableService.setShort(0, (short) 10)));
            shortCases.add(BindVarTuple.ok("zero", """
                    id\ttid\trid
                    1\t0\t101
                    2\t0\t100
                    2\t0\t101
                    """, bindVariableService -> bindVariableService.setShort(0, (short) 0)));
            shortCases.add(BindVarTuple.ok("value again", "id\ttid\trid\n" + TRADE_10, bindVariableService -> bindVariableService.setShort(0, (short) 10)));
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM (SELECT id::short id, x FROM trades) t JOIN xs x ON x.k = o.k RIGHT JOIN refunds r ON r.k = t.x WHERE t.id = $1) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .sizeMayVary()
                    .assertBinds(shortCases);
            bindVariableService.clear();
        });
    }

    // The computed INT t.id is NULL in the rows that the RIGHT join NULL-extends, so per outer row
    // WHERE t.id > o.k drops them, as it does on a table column, and for the NULL o.k of order 3 it
    // holds for no row at all. Trade 10 matches refund 100 for o.k = 1.
    @Test
    public void testLateralCorrelatedInnerOnBeforeRightJoinWithComputedColumnCorrelatedFilter() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            execute("INSERT INTO orders VALUES (3, NULL, 3::timestamp)");
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM (SELECT id + 0 id, x FROM trades) t JOIN xs x ON x.k = o.k RIGHT JOIN refunds r ON t.x = r.k WHERE t.id > o.k) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            """);
        });
    }

    // A sub-query column that the sub-query computes, that a set operation may widen or that a join
    // may take from any of its tables. Per outer row, the RIGHT or FULL join NULL-extends trades for
    // refund 101 of order 1 and for both refunds of order 2, which xs does not match: NULL in i, d
    // and v, false in f and 0 in s. A filter that drops those rows returns 1 10 100, and one that
    // keeps them returns them, with trade 10 when it holds for it too.
    @Test
    public void testLateralCorrelatedInnerOnBeforeRightJoinWithComputedColumnFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE trades (id INT, x INT, s SHORT, f BOOLEAN, i INT, d DOUBLE, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE refunds (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE xs (k INT, v INT)");
            execute("INSERT INTO orders VALUES (1, 1, 1::timestamp), (2, 2, 2::timestamp)");
            execute("INSERT INTO trades VALUES (10, 1, 10, true, 10, 10.0, 'abc', 1::timestamp)");
            execute("INSERT INTO refunds VALUES (100, 1, 1::timestamp), (101, 2, 2::timestamp)");
            execute("INSERT INTO xs VALUES (1, 100)");
            final String[] tables = {
                    "(SELECT id, x, s::short s, f::boolean f, i + 0 i, d * 1 d, concat(v, '') v FROM trades)",
                    "(SELECT id, x, s, f, i, d, v FROM trades UNION ALL SELECT id, x, s, f, i, d, v FROM trades WHERE id < 0)",
                    "(SELECT tr.id, tr.x, tr.s, tr.f, tr.i, tr.d, tr.v FROM trades tr CROSS JOIN long_sequence(1) u)"
            };
            for (String table : tables) {
                for (String join : new String[]{"RIGHT", "FULL"}) {
                    final String body = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM " + table + " t JOIN xs x ON x.k = o.k " + join + " JOIN refunds r ON t.x = r.k WHERE ";
                    for (String filter : new String[]{"t.s != 0", "t.f = true", "t.i IS NOT NULL", "t.d > 0.5", "t.v = 'abc'", "t.i > x.v - 1000"}) {
                        assertQuery(body + filter + ") l ORDER BY 1, 2, 3")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        id\ttid\trid
                                        1\t10\t100
                                        """);
                    }
                    final String[][] cases = {
                            {"t.s = 0", UNMATCHED_REFUNDS},
                            {"t.s IS NOT NULL", TRADE_10_AND_UNMATCHED_REFUNDS},
                            {"t.f = false", UNMATCHED_REFUNDS},
                            {"t.i IS NULL", UNMATCHED_REFUNDS},
                            {"t.d != 0.5", TRADE_10_AND_UNMATCHED_REFUNDS},
                            {"t.v != 'a'", TRADE_10_AND_UNMATCHED_REFUNDS},
                            // NULL - 1000 is NULL, and 0 > NULL is false
                            {"t.s > x.v - 1000", TRADE_10}
                    };
                    for (String[] c : cases) {
                        assertQuery(body + c[0] + ") l ORDER BY 1, 2, 3")
                                .noLeakCheck()
                                .sizeMayVary()
                                .returns("id\ttid\trid\n" + c[1]);
                    }
                }
            }
        });
    }

    // Per outer row, IN with non-NULL values drops the refunds that match no trade. The NULL k of
    // order 3 matches the NULL key of xs, and the NULL x of trade 15 matches refund 102. As = matches
    // NULL to NULL, IN with a NULL value keeps the refunds that match no trade, and trade NULL, which
    // refund 100 matches for o.k = 1 and NULL. NOT IN keeps the refunds that match no trade too.
    @Test
    public void testLateralCorrelatedInnerOnBeforeRightJoinWithInFilter() throws Exception {
        assertMemoryLeak(() -> {
            createNullableCorrelatedRightJoinTables();
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t JOIN xs x ON x.k = o.k RIGHT JOIN refunds r ON r.k = t.x WHERE t.id IN (10, 15)) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            1\t15\t102
                            3\t10\t100
                            3\t15\t102
                            """);
            final String[][] cases = {
                    {
                            "t.id IN (NULL, 10)",
                            """
                                    id\ttid\trid
                                    1\tnull\t100
                                    1\tnull\t101
                                    1\t10\t100
                                    2\tnull\t100
                                    2\tnull\t101
                                    2\tnull\t102
                                    3\tnull\t100
                                    3\tnull\t101
                                    3\t10\t100
                                    """
                    },
                    {
                            "t.id NOT IN (10, 15)",
                            """
                                    id\ttid\trid
                                    1\tnull\t100
                                    1\tnull\t101
                                    2\tnull\t100
                                    2\tnull\t101
                                    2\tnull\t102
                                    3\tnull\t100
                                    3\tnull\t101
                                    """
                    }
            };
            for (String[] c : cases) {
                final String sql = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t JOIN xs x ON x.k = o.k RIGHT JOIN refunds r ON r.k = t.x WHERE " + c[0] + ") l ORDER BY 1, 2, 3";
                assertQuery(sql)
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns(c[1]);
            }
        });
    }

    // The ON clause of the RIGHT or FULL join reads the outer row through refunds only. Per outer
    // row, the refund with the outer key matches every trade, refund 102 for the NULL key of order 3,
    // as = matches NULL to NULL, and t.id > 0 drops trade NULL and the unmatched refunds. Without the
    // filter, both joins keep the unmatched refunds, refund 103 for every outer row, and as every
    // trade finds a refund, the FULL join returns the rows of the RIGHT join.
    @Test
    public void testLateralCorrelatedOuterJoinOnOnlyOuterKey() throws Exception {
        assertMemoryLeak(() -> {
            createNullableCorrelatedRightJoinTables();
            execute("INSERT INTO refunds VALUES (103, 5, 4::timestamp)");
            for (String join : new String[]{"RIGHT", "FULL"}) {
                final String body = "SELECT t.id tid, r.id rid FROM trades t " + join + " JOIN refunds r ON r.k = o.k";
                assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (" + body + " WHERE t.id > 0) l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                id\ttid\trid
                                1\t10\t100
                                1\t12\t100
                                1\t15\t100
                                2\t10\t101
                                2\t12\t101
                                2\t15\t101
                                3\t10\t102
                                3\t12\t102
                                3\t15\t102
                                """);
                assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (" + body + ") l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns("""
                                id\ttid\trid
                                1\tnull\t100
                                1\tnull\t101
                                1\tnull\t102
                                1\tnull\t103
                                1\t10\t100
                                1\t12\t100
                                1\t15\t100
                                2\tnull\t100
                                2\tnull\t101
                                2\tnull\t102
                                2\tnull\t103
                                2\t10\t101
                                2\t12\t101
                                2\t15\t101
                                3\tnull\t100
                                3\tnull\t101
                                3\tnull\t102
                                3\tnull\t103
                                3\t10\t102
                                3\t12\t102
                                3\t15\t102
                                """);
            }
        });
    }

    // Every table written before a RIGHT or FULL join runs before it, also when the outer-ref join of
    // a LATERAL sub-query precedes the join, whatever else the level joins: a non-equi condition on
    // trades, a LEFT join that reads trades after it, a RIGHT or INNER join after it, and a CROSS join
    // before it. xs holds the keys of refunds 100 and 102 only. The data is that of
    // testLateralCorrelatedOuterJoinOnOnlyOuterKey().
    @Test
    public void testLateralCorrelatedOuterJoinRunsAfterItsPrefix() throws Exception {
        assertMemoryLeak(() -> {
            createNullableCorrelatedRightJoinTables();
            execute("INSERT INTO refunds VALUES (103, 5, 4::timestamp)");
            final String everyRefund = """
                    id\ttid\trid
                    1\t10\t100
                    1\t12\t100
                    1\t15\t100
                    2\t10\t101
                    2\t12\t101
                    2\t15\t101
                    3\t10\t102
                    3\t12\t102
                    3\t15\t102
                    """;
            final String refundsInXs = """
                    id\ttid\trid
                    1\t10\t100
                    1\t12\t100
                    1\t15\t100
                    3\t10\t102
                    3\t12\t102
                    3\t15\t102
                    """;
            final String[][] cases = {
                    {"trades t FULL JOIN refunds r ON r.k = o.k AND r.id > t.id WHERE t.id > 0", everyRefund},
                    {"trades t RIGHT JOIN refunds r ON r.k = o.k LEFT JOIN refunds r2 ON r2.k = t.x AND r2.k = r.k WHERE t.id > 0", everyRefund},
                    {"trades t RIGHT JOIN refunds r ON r.k = o.k RIGHT JOIN xs x ON x.k = r.k WHERE t.id > 0 AND r.id > 0", refundsInXs},
                    {"trades t RIGHT JOIN refunds r ON r.k = o.k JOIN xs x ON x.k = r.k WHERE t.id > 0", refundsInXs},
                    {"trades t CROSS JOIN xs x RIGHT JOIN refunds r ON r.k = o.k WHERE t.id = 10 AND x.v = 200", """
                            id\ttid\trid
                            1\t10\t100
                            2\t10\t101
                            3\t10\t102
                            """},
            };
            for (String[] c : cases) {
                assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM " + c[0] + ") l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .expectSize()
                        .returns(c[1]);
            }
        });
    }

    // s holds b row 20, which matches no a row. Per outer row, the first two queries return 1 null 20,
    // and the third, whose filter drops that row, returns 1 10 null and 1 11 null, as the ON clause of
    // the FULL join, which reads the second outer column o.k, matches no a row with s.
    @Test
    public void testLateralCorrelatedOuterJoinToCorrelatedSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, x INT)");
            execute("CREATE TABLE a (id INT, k INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 5, 1)");
            execute("INSERT INTO a VALUES (10, 9, 9), (11, 8, 8)");
            execute("INSERT INTO b VALUES (20, 2, 1)");
            final String[] bodies = {
                    "a FULL JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.k = s.x WHERE s.k < o.k",
                    "a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.k = s.x WHERE s.k < o.k",
                    // the ON clause reads o.k, and WHERE drops the rows that the join NULL-extends on a
                    "a FULL JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.k = s.x AND s.k < o.k WHERE a.id > 0",
            };
            final String unmatchedB = """
                    id\taid\tsid
                    1\tnull\t20
                    """;
            final String[] rows = {
                    unmatchedB,
                    unmatchedB,
                    """
                            id\taid\tsid
                            1\t10\tnull
                            1\t11\tnull
                            """
            };
            for (int i = 0; i < bodies.length; i++) {
                assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM " + bodies[i] + ") l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns(rows[i]);
            }
        });
    }

    // Per outer row, count(*) counts both refunds: 2 for each order.
    @Test
    public void testLateralCorrelatedRightJoinOnCount() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            assertQuery("SELECT o.id, l.c FROM orders o JOIN LATERAL (SELECT count(*) c FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            id\tc
                            1\t2
                            2\t2
                            """);
        });
    }

    // Per outer row, the RIGHT JOIN keeps both refunds: 1 10 100, 1 null 101, 2 null 100 and
    // 2 null 101. The unmatched ones carry the outer row of their own copy of the refunds.
    @Test
    public void testLateralCorrelatedRightJoinOn() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            1\t10\t100
                            2\tnull\t100
                            2\tnull\t101
                            """);
        });
    }

    // WHERE t.id IS NULL keeps exactly the refunds that the RIGHT join NULL-extends per outer row.
    @Test
    public void testLateralCorrelatedRightJoinOnIsNullFilter() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            assertQuery("SELECT o.id, l.rid FROM orders o JOIN LATERAL (SELECT r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id IS NULL) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            id\trid
                            1\t101
                            2\t100
                            2\t101
                            """);
        });
    }

    // WHERE t.x = o.k keeps, per outer row, the refund that trade 10 matches for o.k = 1, and drops
    // the refunds that the RIGHT join NULL-extends, as their NULL t.x equals no o.k.
    @Test
    public void testLateralCorrelatedRightJoinOnWhereKey() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.x = o.k) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            """);
        });
    }

    // t.id > 0 drops the refunds that match no trade for every value of $1, so the query needs no
    // check of $1 and runs for a NULL $1 too: t.id >= NULL then holds only for a NULL t.id, which
    // t.id > 0 drops, so no row remains.
    @Test
    public void testLateralCorrelatedRightJoinOnWithBindVariableAndMasterFilter() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            final String sql = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id >= $1 AND t.id > 0) l ORDER BY 1, 2, 3";
            final ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok("non-NULL value", "id\ttid\trid\n1\t10\t100\n", bindVariableService -> bindVariableService.setInt(0, 5)));
            cases.add(BindVarTuple.ok("NULL value", "id\ttid\trid\n", bindVariableService -> bindVariableService.setInt(0, Numbers.INT_NULL)));
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .assertBinds(cases);
            bindVariableService.clear();
            bindVariableService.setInt(0, 5);
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlanNotContaining("Lateral Null Check");
        });
    }

    // NULL >= NULL is true, so WHERE t.id >= $1 drops the refunds that match no trade per outer row
    // only while $1 is not NULL: for $1 = 5, order 1 returns 10 100. For a NULL $1, it keeps 1 null
    // 101, 2 null 100 and 2 null 101. The query compiles once for all three executions, and checks $1
    // at the top of the plan. A cast of NULL keeps them in every execution. < and > are false for a
    // NULL t.id whatever $1 holds, so they drop those refunds for every value, without a check.
    @Test
    public void testLateralCorrelatedRightJoinOnWithBindVariableFilter() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            for (String[] filter : new String[][]{{"t.id >= $1", "5"}, {"$1 <= t.id", "5"}, {"t.id <= $1", "15"}}) {
                final String sql = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE " + filter[0] + ") l ORDER BY 1, 2, 3";
                final int value = Integer.parseInt(filter[1]);
                assertQuery(sql)
                        .noLeakCheck()
                        .expectSize()
                        .assertBinds(nullValueBindCases(value, UNMATCHED_REFUNDS));
                bindVariableService.clear();
                bindVariableService.setInt(0, value);
                assertQuery(sql)
                        .noLeakCheck()
                        .assertsPlanContaining("Lateral Null Check");
            }
            bindVariableService.clear();
            bindVariableService.setInt(0, 5);
            for (String filter : new String[]{"t.id >= NULL::int", "t.id <= CAST(NULL AS INT)"}) {
                assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE " + filter + ") l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns("id\ttid\trid\n" + UNMATCHED_REFUNDS);
            }
            final String gtSql = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id > $1) l ORDER BY 1, 2, 3";
            assertQuery(gtSql)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            """);
            bindVariableService.setInt(0, Numbers.INT_NULL);
            assertQuery(gtSql)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            """);
        });
    }

    // WHERE t.ts <= systimestamp() reads the clock in every row, so the plan cannot check it once per
    // execution, and checks the value of t.id >= $1 instead: for $1 = 5, order 1 returns 10 100. For a
    // NULL $1, t.id >= NULL holds for the refunds that match no trade, and t.ts <= systimestamp()
    // drops them, as their t.ts is NULL, so no row remains.
    @Test
    public void testLateralCorrelatedRightJoinOnWithBindVariableAfterRowVaryingFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTypedCorrelatedRightJoinTables();
            final String sql = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.ts <= systimestamp() AND t.id >= $1) l ORDER BY 1, 2, 3";
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .assertBinds(nullValueBindCases(5, ""));
            bindVariableService.clear();
            bindVariableService.setInt(0, 5);
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlanContaining("Lateral Null Check");
        });
    }

    // A row-varying operand, such as one over systimestamp(), is NULL here: with a NULL $1, dateadd()
    // returns NULL. NULL = NULL and NULL >= NULL are true, so each filter below keeps the refunds
    // that match no trade per outer row: 1 null 101, 2 null 100 and 2 null 101. So does a filter on a
    // computed sub-query column.
    @Test
    public void testLateralCorrelatedRightJoinOnWithRowVaryingNullFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTypedCorrelatedRightJoinTables();
            bindVariableService.clear();
            bindVariableService.setInt(0, Numbers.INT_NULL);
            final String[][] cases = {
                    {"trades", "t.ts <= dateadd('h', $1, systimestamp())"},
                    {"trades", "t.id = CASE WHEN systimestamp() > 0::timestamp THEN NULL::int END"},
                    {"(SELECT id + 0 id, x, ts FROM trades)", "t.id >= CASE WHEN systimestamp() > 0::timestamp THEN NULL::int END"}
            };
            for (String[] c : cases) {
                assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM " + c[0] + " t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE " + c[1] + ") l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns("id\ttid\trid\n" + UNMATCHED_REFUNDS);
            }
            bindVariableService.clear();
        });
    }

    // A text bind variable converts to the type of the column it is compared with: '0.0.0.0' is the
    // NULL of an IPv4 column, and 'NaN' that of a DOUBLE column. Against such a NULL, WHERE keeps the
    // refunds that match no trade per outer row, as it does for a NULL text compared with a SYMBOL
    // column. A value that converts to no NULL drops them: order 1 returns 10 100. The compiled query
    // returns the rows of each value in turn. The plan checks the filter on the NULL of the column
    // type once per execution.
    @Test
    public void testLateralCorrelatedRightJoinOnWithTextBindVariableFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTypedCorrelatedRightJoinTables();
            final String[][] cases = {
                    {"trades", "t.ip >= $1", "1.1.1.1", "0.0.0.0"},
                    {"trades", "t.ip = $1", "1.1.1.1", "0.0.0.0"},
                    {"trades", "t.d >= $1", "5", "NaN"},
                    {"(SELECT id, x, d + 0 d, ts FROM trades)", "t.d >= $1", "5", "NaN"},
                    {"trades", "t.sym = $1", "abc", null}
            };
            for (String[] c : cases) {
                final String sql = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM " + c[0] + " t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE " + c[1] + ") l ORDER BY 1, 2, 3";
                final String value = c[2];
                final String nullText = c[3];
                final ObjList<BindVarTuple> binds = new ObjList<>();
                binds.add(BindVarTuple.ok("non-NULL value", "id\ttid\trid\n" + TRADE_10, bindVariableService -> bindVariableService.setStr(0, value)));
                binds.add(BindVarTuple.ok("NULL text", "id\ttid\trid\n" + UNMATCHED_REFUNDS, bindVariableService -> bindVariableService.setStr(0, nullText)));
                binds.add(BindVarTuple.ok("non-NULL value again", "id\ttid\trid\n" + TRADE_10, bindVariableService -> bindVariableService.setStr(0, value)));
                assertQuery(sql)
                        .noLeakCheck()
                        .expectSize()
                        .assertBinds(binds);
            }
            bindVariableService.clear();
            bindVariableService.setStr(0, "1.1.1.1");
            final String ipSql = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.ip >= $1) l ORDER BY 1, 2, 3";
            assertQuery(ipSql)
                    .noLeakCheck()
                    .assertsPlanContaining("Lateral Null Check checks: [__qdb_null_probe>=$0::string] groups: [[0]]\n");
        });
    }

    // A constant operand needs no check when the query runs: 5::int, abs(-5) and abs(-10) are not
    // NULL, so they drop the refunds that match no trade.
    @Test
    public void testLateralCorrelatedRightJoinOnWithConstantExpressionFilter() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            for (String filter : new String[]{"t.id >= 5::int", "t.id >= abs(-5)", "t.id = abs(-10)"}) {
                final String sql = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE " + filter + ") l ORDER BY 1, 2, 3";
                assertQuery(sql)
                        .noLeakCheck()
                        .expectSize()
                        .withPlanNotContaining("Lateral Null Check")
                        .returns("""
                                id\ttid\trid
                                1\t10\t100
                                """);
            }
        });
    }

    // terminateHere() moves WHERE t.id > o.k into the outer-ref join's ON clause, ahead of the
    // RIGHT join, so it never sees the unmatched refunds. Per outer row, WHERE drops them: t.id
    // is NULL in them, and for the NULL o.k of order 3, t.id > o.k holds for no row at all. A
    // copy of the condition in WHERE drops them after the join; without it, refund 101 reached
    // order 3, as = matches its NULL outer-ref key to the NULL o.k.
    @Test
    public void testLateralCorrelatedRightJoinOnWithCorrelatedMasterFilter() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            execute("INSERT INTO orders VALUES (3, NULL, 3::timestamp)");
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id > o.k) l ORDER BY o.id")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Filter filter: __qdb_outer_ref__0.__qdb_outer_ref__0_k<t.id\n                        Hash Right Outer Join Light")
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            """);
        });
    }

    // WHERE t.id > 0 drops the unmatched refunds per outer row too, so losing them changes nothing.
    @Test
    public void testLateralCorrelatedRightJoinOnWithMasterFilter() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id > 0) l ORDER BY o.id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            """);
        });
    }

    // t.x > 0 in the later INNER join drops the unmatched refunds per outer row too, although
    // = matches their NULL t.x to the NULL key of xs.
    @Test
    public void testLateralCorrelatedRightJoinOnWithMasterFilterInInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            execute("INSERT INTO xs VALUES (NULL, 200)");
            assertQuery("SELECT o.id, l.tid, l.rid, l.v FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid, x.v FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k JOIN xs x ON x.k = t.x AND t.x > 0) l ORDER BY o.id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid\tv
                            1\t10\t100\t100
                            """);
        });
    }

    // The INNER join keyed on t.x does not drop the unmatched refunds: = matches their NULL t.x
    // to the NULL key of xs. Per outer row, o.k = 1 returns 1 10 100 100 and 1 null 101 200, and
    // o.k = 2 returns 2 null 100 200 and 2 null 101 200.
    @Test
    public void testLateralCorrelatedRightJoinOnWithMasterKeyedJoin() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            execute("INSERT INTO xs VALUES (NULL, 200)");
            assertQuery("SELECT o.id, l.tid, l.rid, l.v FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid, x.v FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k JOIN xs x ON x.k = t.x) l ORDER BY 1, 2, 3, 4")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            id\ttid\trid\tv
                            1\tnull\t101\t200
                            1\t10\t100\t100
                            2\tnull\t100\t200
                            2\tnull\t101\t200
                            """);
        });
    }

    // NaN is the NULL of a DOUBLE column, and so are the strings 'NaN' and 'Infinity' that a DOUBLE
    // column reads, and '0.0.0.0' is the NULL of an IPv4 column. = and <= against them keep the
    // refunds that match no trade per outer row: WHERE t.d = NaN returns 1 null 101, 2 null 100 and
    // 2 null 101. IN with them keeps trade 10 too when it lists its value. != against them is IS NOT
    // NULL, which drops those refunds, and so does IN with non-NULL values on a SYMBOL column.
    @Test
    public void testLateralCorrelatedRightJoinOnWithNullConstantFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTypedCorrelatedRightJoinTables();
            final String[][] cases = {
                    {"t.d = NaN", UNMATCHED_REFUNDS},
                    {"t.d <= NaN", UNMATCHED_REFUNDS},
                    {"t.d IN (NaN, 10.0)", TRADE_10_AND_UNMATCHED_REFUNDS},
                    {"t.ip = '0.0.0.0'", UNMATCHED_REFUNDS},
                    {"t.ip IN ('0.0.0.0', '1.1.1.1')", TRADE_10_AND_UNMATCHED_REFUNDS},
                    {"t.d = 'NaN'", UNMATCHED_REFUNDS},
                    {"'NaN' = t.d", UNMATCHED_REFUNDS},
                    {"t.d >= 'NaN'", UNMATCHED_REFUNDS},
                    {"t.d IN ('NaN', 10.0)", TRADE_10_AND_UNMATCHED_REFUNDS},
                    {"t.d = 'Infinity'", UNMATCHED_REFUNDS},
                    {"t.d >= NULL::double", UNMATCHED_REFUNDS},
                    {"t.sym IN (NULL, 'abc')", TRADE_10_AND_UNMATCHED_REFUNDS}
            };
            for (String[] c : cases) {
                assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE " + c[0] + ") l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns("id\ttid\trid\n" + c[1]);
            }
            for (String filter : new String[]{"t.d != NaN", "t.ip != '0.0.0.0'", "t.d != 'NaN'", "t.d = 10.0", "t.sym IN ('abc', 'xyz')"}) {
                assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE " + filter + ") l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                id\ttid\trid
                                1\t10\t100
                                """);
            }
        });
    }

    // NULL >= NULL is true, so per outer row WHERE t.id >= o.k keeps both unmatched refunds for
    // the NULL o.k of order 3: 3 null 100 and 3 null 101. For o.k = 1 it keeps trade 10, and for
    // o.k = 2 nothing.
    @Test
    public void testLateralCorrelatedRightJoinOnWithNullableMasterFilter() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            execute("INSERT INTO orders VALUES (3, NULL, 3::timestamp)");
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id >= o.k) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            3\tnull\t100
                            3\tnull\t101
                            """);
        });
    }

    // The scalar sub-query runs when the query runs. max(id) over no row is NULL, and NULL >= NULL is
    // true, so WHERE keeps the refunds that match no trade per outer row. min(id) is 10, which drops
    // them.
    @Test
    public void testLateralCorrelatedRightJoinOnWithScalarSubQueryFilter() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id >= (SELECT max(id) FROM trades WHERE id > 1000)) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("id\ttid\trid\n" + UNMATCHED_REFUNDS);
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id >= (SELECT min(id) FROM trades)) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            """);
        });
    }

    // The lateral NULL check runs the scalar sub-query and passes its value to the filter, so the
    // sub-query runs once per execution and the filter never uses a value that the check did not
    // see. EXPLAIN opens the cursor, so the check has passed its value on.
    @Test
    public void testLateralCorrelatedRightJoinOnWithScalarSubQueryFilterPlan() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id >= (SELECT min(id) FROM trades)) l")
                    .noLeakCheck()
                    .assertsPlanContaining("""
                            Lateral Null Check checks: [__qdb_null_probe [thread-safe] >= cursor\s
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [min(id)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades [state-shared]] groups: [[0]]
                            """);
        });
    }

    // A sub-query that is NULL in some executions only: each execution returns the rows of the value
    // that the sub-query takes in that execution, see assertScalarSubQueryFilterOfEitherValue().
    @Test
    public void testLateralCorrelatedRightJoinOnWithScalarSubQueryFilterRandomNull() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            assertScalarSubQueryFilterOfEitherValue("t.id >= (SELECT CASE WHEN rnd_int(0, 1, 0) = 0 THEN NULL::int ELSE 0 END FROM long_sequence(1))");
            assertScalarSubQueryFilterOfEitherValue("t.ts >= (SELECT CASE WHEN rnd_int(0, 1, 0) = 0 THEN NULL::timestamp ELSE 0::timestamp END FROM long_sequence(1))");
        });
    }

    // LATEST ON keeps the last quote of each k: 20 for b and 10 for a. Per outer row, t.id = 20
    // keeps trade 20 with refund 101 for order 2, and t.id = 10 keeps trade 10 with refund 100
    // for order 1. Without LATEST ON, the sub-query would read every quote of b, or 5 as the
    // smallest one.
    @Test
    public void testLateralCorrelatedRightJoinOnWithScalarSubQueryLatestByFilter() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            execute("INSERT INTO trades VALUES (20, 2, 2::timestamp)");
            execute("CREATE TABLE quotes (id INT, k SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO quotes VALUES (5, 'b', 1::timestamp), (20, 'b', 2::timestamp), (7, 'a', 3::timestamp), (10, 'a', 4::timestamp)");
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id = (SELECT id FROM quotes WHERE k = 'b' LATEST ON ts PARTITION BY k)) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            2\t20\t101
                            """);
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id = (SELECT min(id) FROM (SELECT id FROM quotes LATEST ON ts PARTITION BY k))) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            """);
        });
    }

    // The sub-query picks trade 20, the only one with id > 10. Per outer row, t.id = 20 and
    // t.id >= 20 keep trade 20 with refund 101 for order 2. The filter must read the value that the
    // sub-query returns with its WHERE, not trade 10, its first row without it.
    @Test
    public void testLateralCorrelatedRightJoinOnWithScalarSubQueryWhereFilter() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            execute("INSERT INTO trades VALUES (20, 2, 2::timestamp)");
            for (String operator : new String[]{"=", ">="}) {
                assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id " + operator + " (SELECT id FROM trades WHERE id > 10 LIMIT 1)) l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                id\ttid\trid
                                2\t20\t101
                                """);
            }
        });
    }

    // The latest trade with x = 1 is trade 10, which refund 100 matches for order 1 only. Trade
    // 20, the latest of all trades, is the value of the sub-query without its WHERE.
    @Test
    public void testLateralCorrelatedRightJoinOnWithScalarSubQueryWhereFilterLatestValue() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            execute("INSERT INTO trades VALUES (20, 2, 2::timestamp)");
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id = (SELECT id FROM trades WHERE x = 1 ORDER BY ts DESC LIMIT 1)) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            """);
        });
    }

    // min(id) over the trades with x >= 2 is 20, with the WHERE in a sub-query under the
    // aggregate. Per outer row, trade 20 matches refund 101 for order 2 only.
    @Test
    public void testLateralCorrelatedRightJoinOnWithScalarSubQueryWhereFilterNestedLimit() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            execute("INSERT INTO trades VALUES (20, 2, 2::timestamp)");
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id = (SELECT min(id) FROM (SELECT id FROM trades WHERE x >= 2 LIMIT 5))) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            2\t20\t101
                            """);
        });
    }

    // Refund 101, the only one with id > 100, has k = 2, so the sub-query picks trade 20, which
    // matches refund 101 for order 2 only. The inner sub-query has a WHERE of its own.
    @Test
    public void testLateralCorrelatedRightJoinOnWithScalarSubQueryWhereFilterNestedSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            execute("INSERT INTO trades VALUES (20, 2, 2::timestamp)");
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id = (SELECT id FROM trades WHERE x = (SELECT k FROM refunds WHERE id > 100 LIMIT 1) LIMIT 1)) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            2\t20\t101
                            """);
        });
    }

    // Only trade 20 has id > 10, so the sub-query returns one row without a LIMIT, and trade 20
    // matches refund 101 for order 2 only.
    @Test
    public void testLateralCorrelatedRightJoinOnWithScalarSubQueryWhereFilterNoLimit() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            execute("INSERT INTO trades VALUES (20, 2, 2::timestamp)");
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id = (SELECT id FROM trades WHERE id > 10)) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            2\t20\t101
                            """);
        });
    }

    // No trade has id > 1000, so the sub-query is NULL, and NULL >= NULL keeps the refunds that match
    // no trade per outer row: refund 101 for o.k = 1, and refund 100 for o.k = 2, where trade 20
    // matches refund 101. Trade 10, the value of the sub-query without its WHERE, must not count.
    @Test
    public void testLateralCorrelatedRightJoinOnWithScalarSubQueryWhereFilterNull() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            execute("INSERT INTO trades VALUES (20, 2, 2::timestamp)");
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id >= (SELECT id FROM trades WHERE id > 1000 LIMIT 1)) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            2\tnull\t100
                            """);
        });
    }

    // The plan of the NULL check shows the sub-query with its WHERE, as the filter runs it, and
    // names the column that the WHERE reads: x is the second column of trades, while the check
    // itself reads only the probe column. EXPLAIN runs the check, so trade 20 keeps the value
    // from being NULL.
    @Test
    public void testLateralCorrelatedRightJoinOnWithScalarSubQueryWhereFilterPlan() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            execute("INSERT INTO trades VALUES (20, 2, 2::timestamp)");
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id = (SELECT id FROM trades WHERE x > 1 LIMIT 1)) l")
                    .noLeakCheck()
                    .assertsPlanContaining("""
                                      limit: 1
                                      filter: 1<x
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades [state-shared]] groups: [[0]]
                            """);
        });
    }

    // A sub-query column that passes a table column through has its type, under the same name or
    // another, so WHERE t.v IS NOT NULL on the INT id drops the refunds that match no trade, per
    // outer row too. A computed SHORT column holds 0 in those refunds, which IS NOT NULL keeps,
    // while t.v > 5 is false for both NULL and 0.
    @Test
    public void testLateralCorrelatedRightJoinOnWithSubQueryColumnFilter() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelatedRightJoinTables();
            for (String table : new String[]{"(SELECT x, id AS v FROM trades)", "(SELECT * FROM (SELECT x, id v FROM trades))"}) {
                assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.v tid, r.id rid FROM " + table + " t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.v IS NOT NULL) l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                id\ttid\trid
                                1\t10\t100
                                """);
            }
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.v tid, r.id rid FROM (SELECT x, id::short v FROM trades) t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.v IS NOT NULL) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            id\ttid\trid
                            1\t0\t101
                            1\t10\t100
                            2\t0\t100
                            2\t0\t101
                            """);
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.v tid, r.id rid FROM (SELECT x, id::short v FROM trades) t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.v > 5) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            """);
        });
    }

    // A sub-query that passes the SHORT, BYTE and BOOLEAN columns of trades through holds 0 or false
    // in them for the refunds that match no trade, as the table does. Per outer row, a filter that
    // holds for 0 or false keeps those refunds: WHERE t.s < 11 returns 1 10 100, 1 null 101,
    // 2 null 100 and 2 null 101. A filter that is false for 0 or false keeps trade 10 only.
    @Test
    public void testLateralCorrelatedRightJoinOnWithSubQueryTypeWithoutNullFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTypedCorrelatedRightJoinTables();
            final String[][] cases = {
                    {"t.s < 11", TRADE_10_AND_UNMATCHED_REFUNDS},
                    {"t.s >= 0", TRADE_10_AND_UNMATCHED_REFUNDS},
                    {"t.s = 0", UNMATCHED_REFUNDS},
                    {"t.s IS NOT NULL", TRADE_10_AND_UNMATCHED_REFUNDS},
                    {"t.b <= 10", TRADE_10_AND_UNMATCHED_REFUNDS},
                    {"t.f = false", UNMATCHED_REFUNDS},
                    {"t.f IS NOT NULL", TRADE_10_AND_UNMATCHED_REFUNDS}
            };
            for (String table : new String[]{"(SELECT id, x, s, b, f FROM trades)", "(SELECT * FROM trades)", "(SELECT * FROM (SELECT id, x, s, b, f FROM trades))"}) {
                for (String[] c : cases) {
                    assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM " + table + " t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE " + c[0] + ") l ORDER BY 1, 2, 3")
                            .noLeakCheck()
                            .sizeMayVary()
                            .returns("id\ttid\trid\n" + c[1]);
                }
                for (String filter : new String[]{"t.s > 5", "t.b >= 1", "t.f = true"}) {
                    assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM " + table + " t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE " + filter + ") l ORDER BY 1, 2, 3")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    id\ttid\trid
                                    1\t10\t100
                                    """);
                }
            }
        });
    }

    // Each filter reads the value of its own scalar sub-query. The two sub-queries have the same
    // function class, min(id) is 20 and min(k) is 1, so a filter that read the value of the other one
    // would drop every row or keep 1 10 100. Two filters in one body, and two bodies with one filter
    // each, tell the sub-queries apart.
    @Test
    public void testLateralCorrelatedRightJoinOnWithTwoScalarSubQueryFilters() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE trades (id INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE refunds (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO orders VALUES (1, 1, 1::timestamp), (2, 2, 2::timestamp), (3, 3, 3::timestamp)");
            execute("INSERT INTO trades VALUES (10, 1, 1::timestamp), (20, 2, 2::timestamp), (30, 3, 3::timestamp)");
            execute("INSERT INTO refunds VALUES (100, 1, 1::timestamp), (101, 2, 2::timestamp), (102, 3, 3::timestamp)");
            final String tradeGuard = "t.id >= (SELECT min(id) FROM trades WHERE x >= 2)";
            final String refundGuard = "t.x >= (SELECT min(k) FROM refunds)";
            final String[][] guardOrders = {{tradeGuard, refundGuard}, {refundGuard, tradeGuard}};
            for (String[] guards : guardOrders) {
                assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE " + guards[0] + " AND " + guards[1] + ") l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                id\ttid\trid
                                2\t20\t101
                                3\t30\t102
                                """);
            }
            for (String[] guards : guardOrders) {
                assertQuery("SELECT o.id, l1.tid, l1.rid, l2.tid, l2.rid FROM orders o"
                        + " JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE " + guards[0] + ") l1"
                        + " JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE " + guards[1] + ") l2"
                        + " ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                id\ttid\trid\ttid1\trid1
                                2\t20\t101\t20\t101
                                3\t30\t102\t30\t102
                                """);
            }
        });
    }

    // BOOLEAN, BYTE and SHORT have no NULL, so the refunds that match no trade hold false or 0 in
    // those columns of trades. Per outer row, a filter that holds for false or 0 keeps them: WHERE
    // t.s < 11 returns 1 10 100, 1 null 101, 2 null 100 and 2 null 101, and WHERE t.s = 0 all but
    // trade 10. A filter that is false for false or 0, such as t.s > 5, keeps trade 10 only.
    @Test
    public void testLateralCorrelatedRightJoinOnWithTypeWithoutNullFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTypedCorrelatedRightJoinTables();
            final String[][] cases = {
                    {"t.s < 11", TRADE_10_AND_UNMATCHED_REFUNDS},
                    {"t.s >= 0", TRADE_10_AND_UNMATCHED_REFUNDS},
                    {"t.s = 0", UNMATCHED_REFUNDS},
                    {"t.s IS NOT NULL", TRADE_10_AND_UNMATCHED_REFUNDS},
                    {"t.s IN (0, 10)", TRADE_10_AND_UNMATCHED_REFUNDS},
                    {"t.b <= 10", TRADE_10_AND_UNMATCHED_REFUNDS},
                    {"t.b IN (0, 10)", TRADE_10_AND_UNMATCHED_REFUNDS},
                    {"t.f = false", UNMATCHED_REFUNDS},
                    {"t.f IS NOT NULL", TRADE_10_AND_UNMATCHED_REFUNDS},
                    {"t.s > r.id - 200", TRADE_10_AND_UNMATCHED_REFUNDS}
            };
            for (String[] c : cases) {
                assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE " + c[0] + ") l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns("id\ttid\trid\n" + c[1]);
            }
            for (String filter : new String[]{"t.s > 5", "5 < t.s", "t.s != 0", "t.s IN (5, 10)", "t.b >= 1", "t.b IN (10)", "t.f = true"}) {
                assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE " + filter + ") l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                id\ttid\trid
                                1\t10\t100
                                """);
            }
        });
    }

    // Per outer row: for o.x = 1, a matches b row 21 and WHERE a.x = 1 drops unmatched row 20.
    // For a NULL o.x, a matches row 23 and WHERE drops the pair, as a.x is 1, while unmatched
    // row 22 has a NULL a.x and stays. The rewrite keys the rows by a.x, which the RIGHT join
    // NULL-extends, so the equality between that key and the outer key that s carries runs in
    // WHERE, after the join, rather than in its ON clause.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKey() throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE a.x = o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Filter filter: s.__qdb_outer_ref__1_x=a.x", "Hash Right Outer Join")
                    .returns("""
                            id\taid\tsid
                            1\t10\t21
                            2\tnull\t22
                            """);
        });
    }

    // testLateralCorrelatedRightJoinSubQueryWhereKey() with s.id > o.x, which drops unmatched row 22
    // of the NULL o.x, as a comparison with NULL is false. The rewrite removes the outer-ref join and
    // keeps the comparison on o.x, after the lateral join. The RIGHT join makes that join a full-fat
    // hash join, which hashes the body, so the comparison runs for each row of the join.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyAndComparison() throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE a.x = o.x AND s.id > o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: o.x<l.sid", "Hash Join", "Filter filter: s.__qdb_outer_ref__1_x=a.x", "Hash Right Outer Join Light")
                    .returns("""
                            id\taid\tsid
                            1\t10\t21
                            """);
        });
    }

    // Per outer row, count(*) counts a rows 0 and 10 with b row 21 for o.x = 1, nothing for a NULL
    // o.x, and a row 11 with b row 24 for o.x = 3.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyAndComparisonCount() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.n FROM o JOIN LATERAL (SELECT count(*) n FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE a.x = o.x AND s.id > o.x) l ORDER BY 1")
                    .noLeakCheck()
                    .returns("""
                            id\tn
                            1\t2
                            2\t0
                            3\t1
                            """);
        });
    }

    // c row 100 matches b row 21; no c row matches b row 24.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyAndComparisonLaterInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.aid, l.sid, l.cid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid, c.id cid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k JOIN c ON c.sid = s.id WHERE a.x = o.x AND s.id > o.x) l ORDER BY 1, 2, 3, 4")
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid\tcid
                            1\t0\t21\t100
                            1\t10\t21\t100
                            """);
        });
    }

    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyAndComparisonLeftLateral() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o LEFT JOIN LATERAL (SELECT a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE a.x = o.x AND s.id > o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            1\t0\t21
                            1\t10\t21
                            2\tnull\tnull
                            3\t11\t24
                            """);
        });
    }

    // The reference runs the body once per outer row, with its o.x as a constant.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyAndComparisonMatchesPerOuterRow() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertSqlCursors(
                    """
                            SELECT * FROM (
                                SELECT 1 id, a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = 1) s ON a.x = s.k WHERE a.x = 1 AND s.id > 1
                                UNION ALL
                                SELECT 2 id, a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = NULL) s ON a.x = s.k WHERE a.x = NULL AND s.id > NULL
                                UNION ALL
                                SELECT 3 id, a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = 3) s ON a.x = s.k WHERE a.x = 3 AND s.id > 3
                            ) ORDER BY 1, 2, 3""",
                    "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE a.x = o.x AND s.id > o.x) l ORDER BY 1, 2, 3"
            );
        });
    }

    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyAndComparisonMoreRows() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE a.x = o.x AND s.id > o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            1\t0\t21
                            1\t10\t21
                            3\t11\t24
                            """);
        });
    }

    // Per outer row, ORDER BY s.id, a.id LIMIT 1 keeps the first of a rows 0 and 10 with b row 21
    // for o.x = 1, and a row 11 with b row 24 for o.x = 3.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyAndComparisonOrderByLimit() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE a.x = o.x AND s.id > o.x ORDER BY s.id, a.id LIMIT 1) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            1\t0\t21
                            3\t11\t24
                            """);
        });
    }

    // a.id > o.x drops a row 0 for o.x = 1.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyAndMasterComparison() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE a.x = o.x AND a.id > o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            1\t10\t21
                            3\t11\t24
                            """);
        });
    }

    // o.x > 1 keeps the rows of o.x = 3 only.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyAndOuterOnlyCondition() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE a.x = o.x AND o.x > 1) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            3\t11\t24
                            """);
        });
    }

    // Per outer row, count(*) counts 1 for o.x = 1 and for a NULL o.x, and 0 for o.x = 3, whose s
    // is empty.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyCount() throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyTables();
            assertQuery("SELECT o.id, l.c FROM o JOIN LATERAL (SELECT count(*) c FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE a.x = o.x) l ORDER BY 1")
                    .noLeakCheck()
                    .returns("""
                            id\tc
                            1\t1
                            2\t1
                            3\t0
                            """);
        });
    }

    // CROSS JOIN c repeats each row of testLateralCorrelatedRightJoinSubQueryWhereKey() for the four c rows.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyLaterCrossJoin() throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyAndLaterJoinTables();
            assertQuery("SELECT o.id, l.aid, l.sid, l.cid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid, c.id cid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k CROSS JOIN c WHERE a.x = o.x) l ORDER BY 1, 2, 3, 4")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid\tcid
                            1\t10\t21\t100
                            1\t10\t21\t101
                            1\t10\t21\t102
                            1\t10\t21\t103
                            2\tnull\t22\t100
                            2\tnull\t22\t101
                            2\tnull\t22\t102
                            2\tnull\t22\t103
                            """);
        });
    }

    // A later FULL join NULL-extends the rows of the RIGHT join with s. Per outer row, it keeps the c
    // rows that match no s row, c rows 100 and 102 for the NULL o.x, whose a.x is NULL, as WHERE
    // a.x = o.x keeps them for the NULL o.x only.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyLaterFullJoin() throws Exception {
        assertRightJoinSubQueryWhereKeyLaterJoin("FULL JOIN c ON c.sid = s.id", """
                id\taid\tsid\tcid
                1\t10\t21\t100
                2\tnull\tnull\t100
                2\tnull\tnull\t102
                2\tnull\t22\t101
                """);
    }

    // The rewrite moves the equality between a.x and the outer key that s carries into WHERE, after
    // the RIGHT join, and the INNER join with c then keeps the c row of each b row that WHERE keeps.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyLaterInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyAndLaterJoinTables();
            assertQuery("SELECT o.id, l.aid, l.sid, l.cid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid, c.id cid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k JOIN c ON c.sid = s.id WHERE a.x = o.x) l ORDER BY 1, 2, 3, 4")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Filter filter: s.__qdb_outer_ref__1_x=a.x", "Hash Right Outer Join")
                    .returns("""
                            id\taid\tsid\tcid
                            1\t10\t21\t100
                            2\tnull\t22\t101
                            """);
        });
    }

    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyLaterInnerJoinCount() throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyAndLaterJoinTables();
            assertQuery("SELECT o.id, l.n FROM o JOIN LATERAL (SELECT count(*) n FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k JOIN c ON c.sid = s.id WHERE a.x = o.x) l ORDER BY 1")
                    .noLeakCheck()
                    .returns("""
                            id\tn
                            1\t1
                            2\t1
                            3\t0
                            """);
        });
    }

    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyLaterInnerJoinLeftLateral() throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyAndLaterJoinTables();
            assertQuery("SELECT o.id, l.aid, l.sid, l.cid FROM o LEFT JOIN LATERAL (SELECT a.id aid, s.id sid, c.id cid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k JOIN c ON c.sid = s.id WHERE a.x = o.x) l ORDER BY 1, 2, 3, 4")
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid\tcid
                            1\t10\t21\t100
                            2\tnull\t22\t101
                            3\tnull\tnull\tnull
                            """);
        });
    }

    // The reference runs the body once per outer row, with its o.x as a constant.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyLaterInnerJoinMatchesPerOuterRow() throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyAndLaterJoinTables();
            assertSqlCursors(
                    """
                            SELECT * FROM (
                                SELECT 1 id, a.id aid, s.id sid, c.id cid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = 1) s ON a.x = s.k JOIN c ON c.sid = s.id WHERE a.x = 1
                                UNION ALL
                                SELECT 2 id, a.id aid, s.id sid, c.id cid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = NULL) s ON a.x = s.k JOIN c ON c.sid = s.id WHERE a.x = NULL
                                UNION ALL
                                SELECT 3 id, a.id aid, s.id sid, c.id cid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = 3) s ON a.x = s.k JOIN c ON c.sid = s.id WHERE a.x = 3
                            ) ORDER BY 1, 2, 3, 4""",
                    "SELECT o.id, l.aid, l.sid, l.cid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid, c.id cid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k JOIN c ON c.sid = s.id WHERE a.x = o.x) l ORDER BY 1, 2, 3, 4"
            );
        });
    }

    // The RIGHT join with c2 is not the next join after the RIGHT join with s, and it still NULL-extends
    // the rows of that join: per outer row, c2 rows 100 and 102 match no s row for the NULL o.x, and
    // WHERE a.x = o.x keeps them, with NULL in every column but those of c2.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyLaterInnerJoinThenRightJoin() throws Exception {
        assertRightJoinSubQueryWhereKeyLaterJoin("JOIN c ON c.sid = s.id RIGHT JOIN c c2 ON c2.sid = s.id", """
                id\taid\tsid\tcid
                1\t10\t21\t100
                2\tnull\tnull\tnull
                2\tnull\tnull\tnull
                2\tnull\t22\t101
                """);
    }

    // No c row has sid = s.id + 100, so the LEFT join keeps each row with a NULL cid.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyLaterLeftJoin() throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyAndLaterJoinTables();
            assertQuery("SELECT o.id, l.aid, l.sid, l.cid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid, c.id cid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k LEFT JOIN c ON c.sid = s.id + 100 WHERE a.x = o.x) l ORDER BY 1, 2, 3, 4")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid\tcid
                            1\t10\t21\tnull
                            2\tnull\t22\tnull
                            """);
        });
    }

    // A later RIGHT join NULL-extends the rows of the RIGHT join with s. Per outer row, it keeps the c
    // rows that match no s row, which WHERE a.x = o.x keeps for the NULL o.x only, as in
    // testLateralCorrelatedRightJoinSubQueryWhereKeyLaterFullJoin().
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyLaterRightJoin() throws Exception {
        assertRightJoinSubQueryWhereKeyLaterJoin("RIGHT JOIN c ON c.sid = s.id", """
                id\taid\tsid\tcid
                1\t10\t21\t100
                2\tnull\tnull\t100
                2\tnull\tnull\t102
                2\tnull\t22\t101
                """);
    }

    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyLaterSpliceJoinFails() throws Exception {
        assertRightJoinSubQueryWhereKeyLaterSpliceJoinFails();
    }

    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyLeftLateral() throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o LEFT JOIN LATERAL (SELECT a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE a.x = o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            1\t10\t21
                            2\tnull\t22
                            3\tnull\tnull
                            """);
        });
    }

    // The rewrite keys the rows by s.k, which the RIGHT join preserves. The equality between s.k
    // and the outer key that s carries must still filter them: b row 23 belongs to the NULL o.x
    // only, although its k is 1.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyOnSlave() throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE s.k = o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t21
                            """);
        });
    }

    // o.ts is TIMESTAMP and a.ts TIMESTAMP_NS, and a condition next to the WHERE key reads o.ts. See
    // createRetypedKeyTables() for the rows of s and a per outer row. a.y < o.ts + 1 (2us for o row 1)
    // keeps a row 10 of a rows 10 and 12, and drops a row 11 of o row 2 (7us against 6us); the NULL
    // o.ts of o row 3 makes it false. a.y > o.ts + 1 keeps a row 12 for o row 1 and a row 11 for o
    // row 2. With the key on s.k, b row 20 joins a row 10, and b row 25 joins a row 14, which
    // a.y < o.ts + 1 keeps but s.k = o.ts drops. No b row that the RIGHT join NULL-extends passes
    // a condition that compares a NULL a.y.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyRetypedKey() throws Exception {
        assertMemoryLeak(() -> {
            createRetypedKeyTables();
            final String rightJoin = "FROM a RIGHT JOIN (SELECT id, k, ts FROM b WHERE ts = o.ts) s ON a.ts = s.k";
            final String[][] cases = {
                    {
                            "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid " + rightJoin + " WHERE a.ts = o.ts AND a.y < o.ts + 1) l ORDER BY 1, 2, 3",
                            "id\taid\tsid\n1\t10\t20\n"
                    },
                    {
                            "SELECT o.id, l.aid, l.sid FROM o LEFT JOIN LATERAL (SELECT a.id aid, s.id sid " + rightJoin + " WHERE a.ts = o.ts AND a.y < o.ts + 1) l ORDER BY 1, 2, 3",
                            "id\taid\tsid\n1\t10\t20\n2\tnull\tnull\n3\tnull\tnull\n"
                    },
                    {
                            "SELECT o.id, l.n FROM o JOIN LATERAL (SELECT count(*) n " + rightJoin + " WHERE a.ts = o.ts AND a.y < o.ts + 1) l ORDER BY 1",
                            "id\tn\n1\t1\n2\t0\n3\t0\n"
                    },
                    {
                            "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid " + rightJoin + " WHERE a.ts = o.ts AND a.y > o.ts + 1) l ORDER BY 1, 2, 3",
                            "id\taid\tsid\n1\t12\t20\n2\t11\t23\n"
                    },
                    {
                            "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid " + rightJoin + " WHERE s.k = o.ts AND a.y < o.ts + 1) l ORDER BY 1, 2, 3",
                            "id\taid\tsid\n1\t10\t20\n"
                    },
            };
            for (String[] c : cases) {
                assertQuery(c[0])
                        .noLeakCheck()
                        .returns(c[1]);
            }
        });
    }

    // testLateralCorrelatedRightJoinSubQueryWhereKeyRetypedKey() with the types swapped: o.ts is
    // TIMESTAMP_NS and a.ts TIMESTAMP, so o.ts + 1 adds a nanosecond. Per outer row, s holds b rows
    // 20 and 21 for o row 1 (1us), b row 23 for o row 2 (5us), and b rows 22 and 24 for the NULL
    // o.ts of o row 3. b row 20 joins a rows 10 and 12, of which only a row 10 (1us) is below
    // 1.000001us; a row 11 (6us) of o row 2 is not below 5.000001us.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyRetypedKeyWidened() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, ts TIMESTAMP_NS)");
            execute("CREATE TABLE a (id INT, ts TIMESTAMP, y TIMESTAMP)");
            execute("CREATE TABLE b (id INT, k TIMESTAMP, ts TIMESTAMP_NS)");
            execute("INSERT INTO o VALUES (1, '2024-01-01T00:00:00.000001000Z'), (2, '2024-01-01T00:00:00.000005000Z'), (3, NULL)");
            execute("""
                    INSERT INTO a VALUES
                    (10, '2024-01-01T00:00:00.000001Z', '2024-01-01T00:00:00.000001Z'),
                    (11, '2024-01-01T00:00:00.000005Z', '2024-01-01T00:00:00.000006Z'),
                    (12, '2024-01-01T00:00:00.000001Z', '2024-01-01T00:00:00.000002Z'),
                    (13, NULL, NULL)
                    """);
            execute("""
                    INSERT INTO b VALUES
                    (20, '2024-01-01T00:00:00.000001Z', '2024-01-01T00:00:00.000001000Z'),
                    (21, '2024-01-01T00:00:00.000009Z', '2024-01-01T00:00:00.000001000Z'),
                    (22, NULL, NULL),
                    (23, '2024-01-01T00:00:00.000005Z', '2024-01-01T00:00:00.000005000Z'),
                    (24, '2024-01-01T00:00:00.000007Z', NULL)
                    """);
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, ts FROM b WHERE ts = o.ts) s ON a.ts = s.k WHERE a.ts = o.ts AND a.y < o.ts + 1) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            """);
        });
    }

    // Each ordered pair of SYMBOL, VARCHAR and STRING as the outer and the inner key type, with a
    // condition next to the WHERE key that reads o.s. Per outer row, s holds b rows 20 and 21 for
    // 'b', b row 23 for 'e', and b rows 22 and 24 for the NULL o.s, as = matches NULL to NULL. b row
    // 20 joins a rows 10 and 12, of which only a row 10 has a.y < 'b'; a row 11 of 'e' has a.y 'z',
    // and a.y < NULL is false. DuckDB, with IS NOT DISTINCT FROM for =, returns 1 10 20, the rows of
    // the same-typed key.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyRetypedStringKey() throws Exception {
        assertMemoryLeak(() -> {
            final String[][] pairs = {
                    {"VARCHAR", "SYMBOL"},
                    {"SYMBOL", "VARCHAR"},
                    {"STRING", "SYMBOL"},
                    {"SYMBOL", "STRING"},
                    {"VARCHAR", "STRING"},
                    {"STRING", "VARCHAR"},
            };
            final String body = "(SELECT a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, s FROM b WHERE s = o.s) s ON a.s = s.k WHERE a.s = o.s AND a.y < o.s) l ORDER BY 1, 2, 3";
            for (String[] pair : pairs) {
                createRetypedStringKeyTables(pair[0], pair[1]);
                assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL " + body)
                        .noLeakCheck()
                        .returns("""
                                id\taid\tsid
                                1\t10\t20
                                """);
                assertQuery("SELECT o.id, l.aid, l.sid FROM o LEFT JOIN LATERAL " + body)
                        .noLeakCheck()
                        .returns("""
                                id\taid\tsid
                                1\t10\t20
                                2\tnull\tnull
                                3\tnull\tnull
                                """);
                execute("DROP TABLE o");
                execute("DROP TABLE a");
                execute("DROP TABLE b");
            }
        });
    }

    // a matches no b row by id, so per outer row every b row of s is unmatched and has a NULL
    // a.x, which WHERE a.x = o.x keeps only for a NULL o.x: rows 22 and 23, but not rows 20 and
    // 21 of o.x = 1.
    @Test
    public void testLateralCorrelatedRightJoinSubQueryWhereKeyUnmatched() throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.id = s.id WHERE a.x = o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            2\tnull\t22
                            2\tnull\t23
                            """);
        });
    }

    // Exercises the model-replacement flag transfer directly via a @TestOnly accessor.
    // The same regression is also covered black-box by the LATERAL-count assertQuery
    // tests; this pins the unit-level contract of replaceAndTransferDependents.
    @Test
    public void testLateralCountModelReplacementLifecycle() {
        QueryModel oldModel = QueryModel.FACTORY.newInstance();
        QueryModel newModel = QueryModel.FACTORY.newInstance();
        oldModel.setLateralCountCoalesceRequired(true);
        QueryColumn template = new QueryColumn().of("cnt", null);
        oldModel.addLateralCountTemplate(template);

        Assert.assertSame(newModel, SqlOptimiser.replaceAndTransferDependentsForTesting(oldModel, newModel));
        Assert.assertTrue(newModel.isLateralCountCoalesceRequired());
        Assert.assertFalse(oldModel.isLateralCountCoalesceRequired());
        Assert.assertEquals(1, newModel.getLateralCountTemplates().size());
        Assert.assertSame(template, newModel.getLateralCountTemplates().getQuick(0));
        Assert.assertEquals(0, oldModel.getLateralCountTemplates().size());
    }

    // The query writes q.id = p.id in q's ON, before the RIGHT join of p: a forward reference
    // with no defined meaning. Inside a LATERAL body it must fail as it does outside one.
    @Test
    public void testLateralInnerOnForwardReferenceToRightJoinSlaveFails() throws Exception {
        assertMemoryLeak(() -> {
            createOuterRefJoinSlaveTables();
            assertException(
                    "SELECT o.ok, s.c FROM o JOIN LATERAL (SELECT count(*) c FROM t JOIN q ON q.id = p.id RIGHT JOIN p ON t.id = p.id WHERE t.v <= o.v) s ORDER BY 1",
                    73,
                    "Invalid column: q.id"
            );
        });
    }

    // The ON clause of the RIGHT join reads both outer queries. Per order 1 and ps row, t.s != 0 drops
    // the refund that matches no trade, and WHERE t.x = o.k drops it too, as its NULL t.x equals no
    // o.k. A computed column behaves as the column that it passes through: t.s holds 0 in the rows
    // that the join NULL-extends, so t.s = 0 holds there but t.x = o.k does not, and trade 10 has
    // s = 10, so no row remains, also after a later INNER join.
    @Test
    public void testLateralNestedCorrelatedRightJoinWithComputedColumnFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE ps (id INT, k INT)");
            execute("CREATE TABLE trades (id INT, x INT, s SHORT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE refunds (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE xs (k INT, v INT)");
            execute("INSERT INTO orders VALUES (1, 1, 1::timestamp), (2, 2, 2::timestamp)");
            execute("INSERT INTO ps VALUES (10, 1), (11, 2)");
            execute("INSERT INTO trades VALUES (10, 1, 10, 1::timestamp)");
            execute("INSERT INTO refunds VALUES (100, 1, 1::timestamp), (101, 2, 2::timestamp)");
            execute("INSERT INTO xs VALUES (1, 100)");
            for (String table : new String[]{"(SELECT id, x, s, id i FROM trades)", "(SELECT id, x, s::short s, id + 0 i FROM trades)"}) {
                final String body = "SELECT o.id, l.pid, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT p.id pid, m.tid, m.rid FROM ps p JOIN LATERAL (SELECT t.id tid, r.id rid FROM "
                        + table + " t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k AND r.id > p.id ";
                assertQuery(body + "WHERE t.x = o.k AND t.s != 0) m) l ORDER BY 1, 2, 3, 4")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                id\tpid\ttid\trid
                                1\t10\t10\t100
                                1\t11\t10\t100
                                """);
                for (String tail : new String[]{"WHERE t.x = o.k AND t.s = 0", "JOIN xs y ON y.k = r.k AND t.i IS NOT NULL WHERE t.x = o.k AND t.s = 0"}) {
                    assertQuery(body + tail + ") m) l ORDER BY 1, 2, 3, 4")
                            .noLeakCheck()
                            .sizeMayVary()
                            .returns("id\tpid\ttid\trid\n");
                }
            }
        });
    }

    // QuestDB's negative LIMIT means "last |N| rows", which compensateLimit cannot
    // express (it emits `__lateral_rn <= limit`, a contradiction for N < 0). It used
    // to silently empty the lateral body; it must now be rejected.
    @Test
    public void testLateralNegativeLimitRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");

            // scalar-count body
            assertException("SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                            + "(SELECT count() c FROM t2 WHERE t2.k = t1.k LIMIT -1) l ON true",
                    93, "negative LIMIT is not supported in a correlated lateral sub-query");
            // plain row body - same defect, no count involved
            assertException("SELECT t1.k, l.x FROM t1 LEFT JOIN LATERAL "
                            + "(SELECT t2.k x FROM t2 WHERE t2.k = t1.k LIMIT -1) l ON true",
                    90, "negative LIMIT is not supported in a correlated lateral sub-query");
            // an UNCORRELATED body is not decorrelated, so negative LIMIT still works
            assertQuery("SELECT t1.k, l.x FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT t2.k x FROM t2 LIMIT -1) l ON true ORDER BY t1.k")
                    .noLeakCheck()
                    .returns("""
                            k\tx
                            1\t2
                            2\t2
                            3\t2
                            """);
            // a non-lateral negative LIMIT is untouched
            assertQuery("SELECT k FROM t2 LIMIT -1").noLeakCheck().expectSize().returns("""
                    k
                    2
                    """);
        });
    }

    // test_fault() in the projection makes the plan below the NULL check throw on close. The
    // NULL check still closes its own copy of the scalar sub-query, and close() reports the
    // failure of the plan.
    @Test
    public void testLateralNullCheckClosesChecksWhenBaseCloseFails() throws Exception {
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE trades (id INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE refunds (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            final String sql = """
                    SELECT l.tid, test_fault() f
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.id tid
                        FROM trades t
                        RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        WHERE t.id >= (SELECT min(id) FROM trades)
                    ) l
                    """;
            TestFaultFunctionFactory.armCloseFailures();
            try {
                final RecordCursorFactory factory = select(sql);
                try {
                    factory.close();
                    Assert.fail("injected close failure expected");
                } catch (RuntimeException e) {
                    Assert.assertSame(TestFaultFunctionFactory.closeFailure(0), e);
                }
            } finally {
                TestFaultFunctionFactory.disarm();
            }
        });
    }

    // The first NULL check holds a test_fault() that throws on close, and the second one fails
    // to compile. The code generator still frees the plan and the first check, and reports the
    // compile failure instead of the close failure.
    @Test
    public void testLateralNullCheckCompileFailureFreesPlan() throws Exception {
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE trades (id INT, x INT, y INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE refunds (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            // each lateral sub-query gets its own NULL check
            final String sql = """
                    SELECT l1.tid, l2.tid
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.id tid
                        FROM trades t
                        RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        WHERE t.id >= (SELECT CASE WHEN test_fault() THEN 0 END FROM long_sequence(1))
                    ) l1
                    JOIN LATERAL (
                        SELECT t.id tid
                        FROM trades t
                        RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        WHERE t.y >= (SELECT CASE WHEN test_fault() THEN 1 END FROM long_sequence(1))
                    ) l2
                    """;
            TestFaultFunctionFactory.armCloseFailures();
            // the filters create test_fault() instances 0 and 1, then the checks create 2 and 3
            TestFaultFunctionFactory.armToFailAfterCompiles(3);
            try {
                select(sql).close();
                Assert.fail("injected compile failure expected");
            } catch (SqlException e) {
                Assert.assertSame(TestFaultFunctionFactory.lastCompileFailure(), e);
                // the close failure of instance 2 shows that the first check was built
                Assert.assertTrue(Arrays.asList(e.getSuppressed()).contains(TestFaultFunctionFactory.closeFailure(2)));
                Assert.assertEquals(TestFaultFunctionFactory.created(), TestFaultFunctionFactory.closeCalls());
            } finally {
                TestFaultFunctionFactory.disarm();
            }
        });
    }

    @Test
    public void testLateralRequiresSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            assertQuery("SELECT * FROM orders o JOIN LATERAL trades t")
                    .fails(36, "LATERAL requires a subquery");
        });
    }

    // A LIMIT BELOW the aggregate caps the counted input; it can never drop the
    // aggregate row, so the count must degrade to 0, not NULL, and the guard
    // must not include it.
    @Test
    public void testLateralScalarCountBelowAggregateLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");

            String sql = "SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM (SELECT k FROM t2 WHERE t2.k = t1.k LIMIT :m) z) l "
                    + "ON true ORDER BY t1.k";

            // emptied input still aggregates to a 0 row - never NULL
            bindVariableService.setLong("m", 0);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t0
                    2\t0
                    3\t0
                    """);

            bindVariableService.setLong("m", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t1
                    2\t1
                    3\t0
                    """);

            // compile-time constant below the aggregate folds the same way
            assertQuery("SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM (SELECT k FROM t2 WHERE t2.k = t1.k LIMIT 0) z) l "
                    + "ON true ORDER BY t1.k")
                    .noLeakCheck()
                    .returns("""
                            k\tc
                            1\t0
                            2\t0
                            3\t0
                            """);

            // an outer-column LIMIT below the aggregate needs no guard, so it is
            // supported: each outer row caps its own counted input (k-1 rows)
            assertQuery("SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM (SELECT k FROM t2 WHERE t2.k = t1.k LIMIT t1.k - 1) z) l "
                    + "ON true ORDER BY t1.k")
                    .noLeakCheck()
                    .returns("""
                            k\tc
                            1\t0
                            2\t1
                            3\t0
                            """);
        });
    }

    // Same below-aggregate LIMIT, per-side-eligible shape: the chain LIMIT
    // disqualifies per-side push and the fallback applies it per outer row
    // beneath the count.
    @Test
    public void testLateralScalarCountBelowAggregateLimitPerSideShape() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");
            execute("CREATE TABLE t3 (k INT)");
            execute("INSERT INTO t3 VALUES (1), (2), (3)");

            String sql = "SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM ("
                    + "   SELECT t2.k FROM t2 JOIN (SELECT k FROM t3 WHERE t3.k = t1.k) s ON s.k = t2.k LIMIT :m"
                    + " ) z) l ON true ORDER BY t1.k";

            bindVariableService.setLong("m", 0);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t0
                    2\t0
                    3\t0
                    """);

            bindVariableService.setLong("m", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t1
                    2\t1
                    3\t0
                    """);
        });
    }

    // A bind variable is only runtime-constant: a cached plan may be re-executed with a
    // different value, so whether the aggregate row survives the LIMIT cannot be folded
    // at compile time. It is emitted as a SQL guard and re-evaluated per execution.
    @Test
    public void testLateralScalarCountBindVariableLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");

            String sql = "SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 WHERE t2.k = t1.k LIMIT :lim) l ON true ORDER BY t1.k";

            // row-preserving value: no-match outer row must still yield 0
            bindVariableService.setLong("lim", 2);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t2
                    2\t1
                    3\t0
                    """);

            // LIMIT 0 empties the body, so NULL - and this must be observed on the SAME
            // statement, proving the decision is not baked into the compiled plan
            bindVariableService.setLong("lim", 0);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\tnull
                    2\tnull
                    3\tnull
                    """);

            // and back again, to rule out a one-way latch
            bindVariableService.setLong("lim", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t2
                    2\t1
                    3\t0
                    """);
        });
    }

    @Test
    public void testLateralScalarCountBindVariableLimitCachedPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");

            final ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok(
                    "limit keeps the aggregate row",
                    "k\tc\n1\t2\n2\t1\n3\t0\n",
                    bindVariableService -> bindVariableService.setLong("lim", 2)
            ));
            cases.add(BindVarTuple.ok(
                    "limit drops the aggregate row",
                    "k\tc\n1\tnull\n2\tnull\n3\tnull\n",
                    bindVariableService -> bindVariableService.setLong("lim", 0)
            ));
            cases.add(BindVarTuple.ok(
                    "limit keeps the aggregate row again",
                    "k\tc\n1\t2\n2\t1\n3\t0\n",
                    bindVariableService -> bindVariableService.setLong("lim", 1)
            ));

            assertQuery("SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 WHERE t2.k = t1.k LIMIT :lim) l ON true ORDER BY t1.k")
                    .noLeakCheck()
                    .assertBinds(cases);

            final ObjList<BindVarTuple> outerWhereCases = new ObjList<>();
            outerWhereCases.add(BindVarTuple.ok(
                    "outer WHERE keeps the unmatched group",
                    "k\n3\n",
                    bindVariableService -> bindVariableService.setLong("lim", 2)
            ));
            outerWhereCases.add(BindVarTuple.ok(
                    "outer WHERE drops NULL counts",
                    "k\n",
                    bindVariableService -> bindVariableService.setLong("lim", 0)
            ));
            outerWhereCases.add(BindVarTuple.ok(
                    "outer WHERE keeps the unmatched group again",
                    "k\n3\n",
                    bindVariableService -> bindVariableService.setLong("lim", 1)
            ));

            assertQuery("SELECT t1.k FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 WHERE t2.k = t1.k LIMIT :lim) l ON true "
                    + "WHERE l.c = 0 ORDER BY t1.k")
                    .noLeakCheck()
                    .assertBinds(outerWhereCases);
        });
    }

    @Test
    public void testLateralScalarCountBindVariableLimitNegativeRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");
            execute("CREATE TABLE t3 (k INT, v INT)");
            execute("INSERT INTO t3 VALUES (1, 10), (1, 20), (2, 30)");

            final ObjList<BindVarTuple> countCases = new ObjList<>();
            countCases.add(BindVarTuple.ok(
                    "positive limit keeps the aggregate row",
                    "k\tc\n1\t2\n2\t1\n3\t0\n",
                    bindVariableService -> bindVariableService.setLong("lim", 2)
            ));
            countCases.add(BindVarTuple.fails(
                    "negative limit is rejected at run time",
                    "negative LIMIT is not supported in a correlated lateral sub-query",
                    bindVariableService -> bindVariableService.setLong("lim", -1)
            ));
            countCases.add(BindVarTuple.ok(
                    "the factory survives the rejection",
                    "k\tc\n1\t2\n2\t1\n3\t0\n",
                    bindVariableService -> bindVariableService.setLong("lim", 1)
            ));
            countCases.add(BindVarTuple.ok(
                    "null limit keeps dropping the aggregate row",
                    "k\tc\n1\tnull\n2\tnull\n3\tnull\n",
                    bindVariableService -> bindVariableService.setLong("lim", Numbers.LONG_NULL)
            ));

            assertQuery("SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 WHERE t2.k = t1.k LIMIT :lim) l ON true ORDER BY t1.k")
                    .noLeakCheck()
                    .assertBinds(countCases);

            final ObjList<BindVarTuple> rowCases = new ObjList<>();
            rowCases.add(BindVarTuple.ok(
                    "positive limit keeps the first row per key",
                    "k\tv\n1\t10\n2\t30\n",
                    bindVariableService -> bindVariableService.setLong("lim", 1)
            ));
            rowCases.add(BindVarTuple.fails(
                    "negative limit is rejected at run time",
                    "negative LIMIT is not supported in a correlated lateral sub-query",
                    bindVariableService -> bindVariableService.setLong("lim", -1)
            ));
            rowCases.add(BindVarTuple.ok(
                    "the factory survives the rejection",
                    "k\tv\n1\t10\n1\t20\n2\t30\n",
                    bindVariableService -> bindVariableService.setLong("lim", 2)
            ));

            assertQuery("SELECT t1.k, l.v FROM t1 JOIN LATERAL "
                    + "(SELECT v FROM t3 WHERE t3.k = t1.k ORDER BY v LIMIT :lim) l ORDER BY t1.k, l.v")
                    .noLeakCheck()
                    .assertBinds(rowCases);
        });
    }

    // An outer WHERE over the compensated count hoists into the carrier scope with
    // the same run-time guard the column materializes with: when the bind-variable
    // LIMIT empties the body, the predicate judges NULL, not an unguarded 0.
    @Test
    public void testLateralScalarCountBindVariableLimitOuterWhere() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");

            String sql = "SELECT t1.k FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 WHERE t2.k = t1.k LIMIT :lim) l ON true "
                    + "WHERE l.c = 0 ORDER BY t1.k";

            bindVariableService.setLong("lim", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    k
                    3
                    """);

            // LIMIT 0 empties the body: every count is NULL and NULL = 0 keeps no row
            bindVariableService.setLong("lim", 0);
            assertQuery(sql).noLeakCheck().returns("""
                    k
                    """);

            bindVariableService.setLong("lim", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    k
                    3
                    """);
        });
    }

    // The count layer's OWN LIMIT on a per-side-push-eligible body (correlation
    // only inside a join branch, uncorrelated main chain). Per-side push cannot
    // key the chain per outer row, so a chain LIMIT disqualifies it and the body
    // takes the general path, which partitions the LIMIT per outer row and
    // guards the coalesce(count, 0) compensation.
    @Test
    public void testLateralScalarCountChainBindVarLimitPerSideShape() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");
            execute("CREATE TABLE t3 (k INT)");
            execute("INSERT INTO t3 VALUES (1), (2), (3)");

            String sql = "SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 "
                    + " JOIN (SELECT k FROM t3 WHERE t3.k = t1.k) s ON s.k = t2.k "
                    + " LIMIT :m) l ON true ORDER BY t1.k";

            // row-preserving value: every outer row keeps its own count, and the
            // no-match outer row yields 0, not NULL
            bindVariableService.setLong("m", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t2
                    2\t1
                    3\t0
                    """);

            // LIMIT 0 empties the body per outer row, so NULL - observed on the
            // SAME statement, proving the guard is not baked into the cached plan
            bindVariableService.setLong("m", 0);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\tnull
                    2\tnull
                    3\tnull
                    """);

            // and back again, to rule out a one-way latch
            bindVariableService.setLong("m", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t2
                    2\t1
                    3\t0
                    """);

            // compile-time decidable values fold without a guard on the same shape
            assertQuery("SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 "
                    + " JOIN (SELECT k FROM t3 WHERE t3.k = t1.k) s ON s.k = t2.k "
                    + " LIMIT 1) l ON true ORDER BY t1.k")
                    .noLeakCheck()
                    .returns("""
                            k\tc
                            1\t2
                            2\t1
                            3\t0
                            """);
            assertQuery("SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 "
                    + " JOIN (SELECT k FROM t3 WHERE t3.k = t1.k) s ON s.k = t2.k "
                    + " LIMIT 0) l ON true ORDER BY t1.k")
                    .noLeakCheck()
                    .returns("""
                            k\tc
                            1\tnull
                            2\tnull
                            3\tnull
                            """);
        });
    }

    // Two-sided runtime LIMIT on the count layer of a per-side-push-eligible
    // body: the guard mirrors the row_number filter at row 1, so lo >= 1 drops
    // the single aggregate row and NULL survives.
    @Test
    public void testLateralScalarCountChainTwoSidedBindVarLimitPerSideShape() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");
            execute("CREATE TABLE t3 (k INT)");
            execute("INSERT INTO t3 VALUES (1), (2), (3)");

            String sql = "SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 "
                    + " JOIN (SELECT k FROM t3 WHERE t3.k = t1.k) s ON s.k = t2.k "
                    + " LIMIT :lo,:hi) l ON true ORDER BY t1.k";

            bindVariableService.setLong("lo", 0);
            bindVariableService.setLong("hi", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t2
                    2\t1
                    3\t0
                    """);

            // (lo, hi] = (1, 2] skips the single aggregate row -> NULL, on the
            // same statement
            bindVariableService.setLong("lo", 1);
            bindVariableService.setLong("hi", 2);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\tnull
                    2\tnull
                    3\tnull
                    """);

            bindVariableService.setLong("lo", 0);
            bindVariableService.setLong("hi", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t2
                    2\t1
                    3\t0
                    """);
        });
    }

    // A LIMIT on a join branch INSIDE the lateral body never drops the aggregate
    // row, so it must not produce an outer NULL guard. The count layer's own rows
    // are limited by the branch, but count() over an empty join still returns 0.
    // Per-side push path: INNER branch, uncorrelated main chain.
    @Test
    public void testLateralScalarCountJoinBranchBindVarLimitPerSide() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");
            execute("CREATE TABLE t3 (k INT)");
            execute("INSERT INTO t3 VALUES (1), (2), (3)");

            String sql = "SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 "
                    + " JOIN (SELECT k FROM t3 WHERE t3.k = t1.k LIMIT :n) s ON s.k = t2.k"
                    + ") l ON true ORDER BY t1.k";

            bindVariableService.setLong("n", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t2
                    2\t1
                    3\t0
                    """);

            // emptying the BRANCH empties the join, but the aggregate row survives:
            // count() = 0, never NULL; same statement, so a leaked guard baked into
            // the cached plan would flip the whole column here
            bindVariableService.setLong("n", 0);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t0
                    2\t0
                    3\t0
                    """);
        });
    }

    // Two-sided branch LIMIT with lo=1: drops the branch's first row, but the
    // aggregate row above the join is untouched.
    @Test
    public void testLateralScalarCountJoinBranchTwoSidedBindVarLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");
            execute("CREATE TABLE t3 (k INT)");
            execute("INSERT INTO t3 VALUES (1), (1), (2)");

            String sql = "SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 "
                    + " LEFT JOIN (SELECT k FROM t3 WHERE t3.k = t1.k LIMIT :lo,:hi) s ON s.k = t2.k "
                    + " WHERE t2.k = t1.k) l ON true ORDER BY t1.k";

            bindVariableService.setLong("lo", 1);
            bindVariableService.setLong("hi", 2);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t2
                    2\t1
                    """);
        });
    }

    // Join-branch push path: LEFT branch plus correlated main-chain WHERE.
    @Test
    public void testLateralScalarCountLeftJoinBranchBindVarLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");
            execute("CREATE TABLE t3 (k INT)");
            execute("INSERT INTO t3 VALUES (1), (2), (3)");

            String sql = "SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 "
                    + " LEFT JOIN (SELECT k FROM t3 WHERE t3.k = t1.k LIMIT :n) s ON s.k = t2.k "
                    + " WHERE t2.k = t1.k) l ON true ORDER BY t1.k";

            // LEFT branch: emptying it must not change the count of t2 rows
            bindVariableService.setLong("n", 0);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t2
                    2\t1
                    3\t0
                    """);

            bindVariableService.setLong("n", 2);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t2
                    2\t1
                    3\t0
                    """);
        });
    }

    // The pinned contract in testLateralScalarCountOuterColumnLimitRejected: only a
    // LIMIT over the scalar count itself is rejected. The same LIMIT on a NON-count
    // join branch inside the body must stay supported.
    @Test
    public void testLateralScalarCountNonCountBranchOuterColumnLimitSupported() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT, n INT)");
            execute("INSERT INTO t1 VALUES (1,1), (2,1), (3,1)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");
            execute("CREATE TABLE t3 (k INT)");
            execute("INSERT INTO t3 VALUES (1), (2), (3)");

            assertQuery("SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 "
                    + " LEFT JOIN (SELECT k FROM t3 WHERE t3.k = t1.k LIMIT t1.n) s ON s.k = t2.k "
                    + " WHERE t2.k = t1.k) l ON true ORDER BY t1.k")
                    .noLeakCheck()
                    .returns("""
                            k\tc
                            1\t2
                            2\t1
                            3\t0
                            """);
        });
    }

    // Regression: a row-preserving LIMIT must keep the coalesce(count, 0) compensation
    // so a no-match outer row still yields 0 rather than NULL. Covers non-literal forms
    // (1+1, abs(1)) which the head revision silently dropped the compensation for.
    @Test
    public void testLateralScalarCountNonLiteralPositiveLimitKeepsCoalesce() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");

            for (String limit : new String[]{"LIMIT 1", "LIMIT 2", "LIMIT 1+1", "LIMIT abs(1)", "LIMIT 0,1", ""}) {
                assertQuery("SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                        + "(SELECT count() c FROM t2 WHERE t2.k = t1.k " + limit + ") l ON true ORDER BY t1.k")
                        .noLeakCheck()
                        .returns("""
                                k\tc
                                1\t2
                                2\t1
                                3\t0
                                """);
            }
        });
    }

    @Test
    public void testLateralScalarCountOuterColumnLimitExpressionWrapperFallsBack() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT, n INT)");
            execute("INSERT INTO t1 VALUES (1, 2), (2, 1), (3, 1), (4, 0)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2), (4)");

            assertQuery("""
                    SELECT t1.k, l.v
                    FROM t1
                    LEFT JOIN LATERAL (
                        SELECT v
                        FROM (
                            SELECT count(*) + 2 AS v
                            FROM t2
                            WHERE t2.k = t1.k
                        ) counted
                        LIMIT t1.n
                    ) l ON true
                    ORDER BY t1.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\tv
                            1\t4
                            2\t3
                            3\tnull
                            4\tnull
                            """);

            assertQuery("""
                    SELECT t1.k, l.v
                    FROM t1
                    CROSS JOIN LATERAL (
                        SELECT v
                        FROM (
                            SELECT count(*) + 2 AS v
                            FROM t2
                            WHERE t2.k = t1.k
                        ) counted
                        LIMIT t1.n
                    ) l
                    ORDER BY t1.k
                    """)
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            k\tv
                            1\t4
                            2\t3
                            """);

            assertQuery("""
                    SELECT t1.k, l.v
                    FROM t1
                    JOIN LATERAL (
                        SELECT v
                        FROM (
                            SELECT count(*) + 2 AS v
                            FROM t2
                            WHERE t2.k = t1.k
                        ) counted
                        LIMIT t1.n
                    ) l ON true
                    ORDER BY t1.k
                    """)
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            k\tv
                            1\t4
                            2\t3
                            """);
        });
    }

    // A LIMIT reading an outer column cannot be guarded: the guard is evaluated in the
    // outer projection, where the body-local rewrite of that reference does not resolve.
    @Test
    public void testLateralScalarCountOuterColumnLimitRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT, n INT)");
            execute("INSERT INTO t1 VALUES (1,2), (2,2), (3,2)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");

            assertException("SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                            + "(SELECT count() c FROM t2 WHERE t2.k = t1.k LIMIT t1.n) l ON true",
                    93, "LIMIT referencing an outer column is not supported over a scalar count");

            // the same LIMIT over a NON-count body stays supported
            assertQuery("SELECT t1.k, l.x FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT t2.k x FROM t2 WHERE t2.k = t1.k ORDER BY t2.k LIMIT t1.n) l ON true ORDER BY t1.k, l.x")
                    .noLeakCheck()
                    .returns("""
                            k\tx
                            1\t1
                            1\t1
                            2\t2
                            3\tnull
                            """);
        });
    }

    // The count layer's OWN LIMIT and a branch LIMIT together: the outer guard
    // must reflect the count layer's LIMIT (:m=0 drops the aggregate row -> NULL),
    // not be displaced by the branch-local one.
    @Test
    public void testLateralScalarCountOwnLimitWithJoinBranchLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");
            execute("CREATE TABLE t3 (k INT)");
            execute("INSERT INTO t3 VALUES (1), (2), (3)");

            String sql = "SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 "
                    + " LEFT JOIN (SELECT k FROM t3 WHERE t3.k = t1.k LIMIT :n) s ON s.k = t2.k "
                    + " WHERE t2.k = t1.k LIMIT :m) l ON true ORDER BY t1.k";

            // LIMIT 0 on the count layer drops the aggregate row: NULL for every
            // outer row, regardless of the branch's LIMIT
            bindVariableService.setLong("m", 0);
            bindVariableService.setLong("n", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\tnull
                    2\tnull
                    3\tnull
                    """);

            bindVariableService.setLong("m", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t2
                    2\t1
                    3\t0
                    """);
        });
    }

    // A limit that provably removes the single aggregate row must NOT be
    // compensated - NULL fill is the correct answer there.
    @Test
    public void testLateralScalarCountRowDroppingLimitYieldsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");

            for (String limit : new String[]{"LIMIT 0", "LIMIT 1-1", "LIMIT 1,2"}) {
                assertQuery("SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                        + "(SELECT count() c FROM t2 WHERE t2.k = t1.k " + limit + ") l ON true ORDER BY t1.k")
                        .noLeakCheck()
                        .returns("""
                                k\tc
                                1\tnull
                                2\tnull
                                3\tnull
                                """);
            }
        });
    }

    // Two correlated branches with independent bind-variable LIMITs. Neither guard
    // may escape onto the outer count; the asymmetric bind sets would expose a
    // last-branch-wins leak ((:n1,:n2)=(1,0) nulls, (0,1) coincidentally passes).
    @Test
    public void testLateralScalarCountTwoJoinBranchesBindVarLimitsPerSide() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2), (3)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (1), (2)");
            execute("CREATE TABLE t3 (k INT)");
            execute("INSERT INTO t3 VALUES (1), (2), (3)");
            execute("CREATE TABLE t4 (k INT)");
            execute("INSERT INTO t4 VALUES (1), (2), (3)");

            String sql = "SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL "
                    + "(SELECT count() c FROM t2 "
                    + " JOIN (SELECT k FROM t3 WHERE t3.k = t1.k LIMIT :n1) s1 ON s1.k = t2.k"
                    + " JOIN (SELECT k FROM t4 WHERE t4.k = t1.k LIMIT :n2) s2 ON s2.k = t2.k"
                    + ") l ON true ORDER BY t1.k";

            bindVariableService.setLong("n1", 0);
            bindVariableService.setLong("n2", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t0
                    2\t0
                    3\t0
                    """);

            bindVariableService.setLong("n1", 1);
            bindVariableService.setLong("n2", 0);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t0
                    2\t0
                    3\t0
                    """);

            bindVariableService.setLong("n1", 1);
            bindVariableService.setLong("n2", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    k\tc
                    1\t2
                    2\t1
                    3\t0
                    """);
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

            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            1\t10.0
                            2\t20.0
                            """);
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

            assertQuery("""
                    SELECT * FROM orders o
                    RIGHT JOIN LATERAL (SELECT * FROM trades WHERE order_id = o.id) t
                    """)
                    .fails(34, "LATERAL is only supported with INNER, LEFT, or CROSS joins");
        });
    }

    // A condition on the outer row alone in the ON clause of a join before a RIGHT or FULL join decides,
    // per outer row, whether the master side of that join is empty. Per outer row, o.x > 1 leaves it
    // empty for o row 1, and the RIGHT or FULL join then NULL-extends every s row, which WHERE a.x =
    // o.x drops, as their NULL a.x is not 1. For the NULL o.x of o row 2 it keeps them, as NULL = NULL
    // is true, and for o row 3, a row 11 matches s row 24, once per c row that the ON clause keeps.
    // The correlated sub-query of s keeps the s rows with a NULL x for o row 2, and o.id > 1 keeps the
    // master side of o row 2, where only s rows 20 and 22 match no a row.
    @Test
    public void testLateralOuterOnlyConditionBeforeRightJoin() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            final String nullExtendedForRow2 = """
                    id\taid\tsid
                    2\tnull\t20
                    2\tnull\t21
                    2\tnull\t22
                    2\tnull\t23
                    2\tnull\t24
                    3\t11\t24
                    """;
            final String[][] cases = {
                    {
                            "a JOIN c ON c.sid = a.id + 11 AND o.x > 1 RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k",
                            """
                                    id\taid\tsid
                                    2\tnull\t22
                                    2\tnull\t23
                                    3\t11\t24
                                    """
                    },
                    {"a JOIN c ON c.sid = a.id + 11 AND o.x > 1 RIGHT JOIN b s ON a.x = s.k", nullExtendedForRow2},
                    {"a JOIN c ON c.sid = a.id + 11 AND o.x > 1 FULL JOIN b s ON a.x = s.k", nullExtendedForRow2},
                    {
                            "a JOIN c ON o.x > 1 RIGHT JOIN b s ON a.x = s.k",
                            """
                                    id\taid\tsid
                                    2\tnull\t20
                                    2\tnull\t21
                                    2\tnull\t22
                                    2\tnull\t23
                                    2\tnull\t24
                                    3\t11\t24
                                    3\t11\t24
                                    3\t11\t24
                                    3\t11\t24
                                    3\t11\t24
                                    """
                    },
                    {
                            "a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b s ON a.x = s.k",
                            """
                                    id\taid\tsid
                                    2\tnull\t20
                                    2\tnull\t22
                                    3\t11\t24
                                    """
                    },
            };
            for (String[] c : cases) {
                assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM " + c[0] + " WHERE a.x = o.x) l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns(c[1]);
            }
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid FROM a RIGHT JOIN b s ON a.x = s.k WHERE a.x = o.x AND o.x > 1) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            3\t11\t24
                            """);
        });
    }

    // The outer-row condition on c's ON clause stays there, and the rewriter rewrites it to the key
    // a.x, which WHERE a.x = o.x equates to the outer key in every row that it keeps: a.id > 0
    // drops the rows that the RIGHT join NULL-extends. Per outer row, only o.x = 3 passes o.x > 1,
    // and a row 11 matches c row 101 and b row 24. With o.id > 1, o.x = 1 fails it, and the NULL
    // o.x leaves a row 12, which matches no b row.
    @Test
    public void testLateralOuterOnlyConditionBeforeRightJoinNullRejectingWhere() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 1 RIGHT JOIN b ON a.x = b.k WHERE a.x = o.x AND a.id > 0) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            3\t11\t24
                            """);
            assertQuery("SELECT o.id, l.n FROM o JOIN LATERAL (SELECT count(*) n FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 1 RIGHT JOIN b ON a.x = b.k WHERE a.x = o.x AND a.id > 0) l ORDER BY 1")
                    .noLeakCheck()
                    .returns("""
                            id\tn
                            1\t0
                            2\t0
                            3\t1
                            """);
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE a.x = o.x AND a.id > 0) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            3\t11\t24
                            """);
        });
    }

    // o.x > 1 on the ON clause of the INNER join after the RIGHT join drops every row of o.x = 1 and
    // of the NULL o.x, and for o.x = 3, the only row with a.x = 3 pairs a row 11 with b row 24, which
    // no c row matches.
    @Test
    public void testLateralOuterOnlyConditionAfterRightJoin() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o LEFT JOIN LATERAL (SELECT a.id aid, b.id sid FROM a RIGHT JOIN b ON a.x = b.k JOIN c ON c.sid = b.id AND o.x > 1 WHERE a.x = o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            1\tnull\tnull
                            2\tnull\tnull
                            3\tnull\tnull
                            """);
        });
    }

    @Test
    public void testLateralOuterOnlyConditionInInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN b ON a.x = b.k AND o.x > 1 WHERE a.x = o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            3\t11\t24
                            """);
        });
    }

    // On timestamped tables: the SPLICE join follows the INNER join whose ON clause reads the outer
    // row alone. The reference runs the body once per outer row, with its o.x as a constant.
    @Test
    public void testLateralOuterOnlyConditionBeforeSpliceJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("CREATE TABLE a (id INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE c (id INT, sid INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE b (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO o VALUES (1, 1), (2, NULL), (3, 3)");
            execute("INSERT INTO a VALUES (10, 1, 1::timestamp), (11, 3, 2::timestamp), (12, NULL, 3::timestamp), (0, 1, 4::timestamp)");
            execute("INSERT INTO c VALUES (100, 21, 1::timestamp), (101, 22, 2::timestamp), (102, 23, 3::timestamp), (103, 11, 4::timestamp)");
            execute("INSERT INTO b VALUES (20, 5, 1::timestamp), (21, 1, 2::timestamp), (24, 3, 5::timestamp)");
            assertSqlCursors(
                    """
                            SELECT * FROM (
                                SELECT 1 id, a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND 1 > 1 SPLICE JOIN b WHERE a.x = 1
                                UNION ALL
                                SELECT 2 id, a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND NULL::int > 1 SPLICE JOIN b WHERE a.x = NULL
                                UNION ALL
                                SELECT 3 id, a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND 3 > 1 SPLICE JOIN b WHERE a.x = 3
                            ) ORDER BY 1, 2, 3""",
                    "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 1 SPLICE JOIN b WHERE a.x = o.x) l ORDER BY 1, 2, 3"
            );
        });
    }

    // c's ON clause reads o.x, and WHERE keys o.x on a column of the table that the RIGHT join joins,
    // or of a later one, which no row before the RIGHT join holds. Per outer row, o.x > 1 leaves the
    // master side of the RIGHT join empty for o.x = 1, so b rows 21 and 23 come NULL-extended, and
    // keeps a row 11 for o.x = 3, which matches b row 24. The rows come from DuckDB, with IS NOT
    // DISTINCT FROM for QuestDB's =. DuckDB does not run the FULL join in a LATERAL sub-query, so its
    // rows come from DuckDB running the body once per outer row. Each query runs twice, and the same
    // compiler then answers the query whose key is on a.
    @Test
    public void testLateralOuterConditionBeforeRightJoinKeyOnSlave() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            final String itemRows = """
                    id\taid\tsid
                    1\tnull\t21
                    1\tnull\t23
                    3\t11\t24
                    """;
            final String[][] cases = {
                    {"SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x", itemRows},
                    // o.x inside the arithmetic of the equality: for o.x = 1, c.sid = a.id + 12 keeps a
                    // rows 10 and 11, and a row 10 matches b rows 21 and 23; for o.x = 3, c.sid = a.id + 14
                    // matches no a row
                    {"SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 + o.x RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x", """
                            id\taid\tsid
                            1\t10\t21
                            1\t10\t23
                            3\tnull\t24
                            """},
                    // o.x compared with a column of the master side, which keeps only a row 11 for o.x = 1
                    // and no a row for o.x = 3
                    {"SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND a.x > o.x RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x", """
                            id\taid\tsid
                            1\tnull\t21
                            1\tnull\t23
                            3\tnull\t24
                            """},
                    {"SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid - 20 = o.x RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x", """
                            id\taid\tsid
                            1\t0\t21
                            1\t0\t23
                            1\t10\t21
                            1\t10\t23
                            3\t11\t24
                            """},
                    // o.x as an argument of BETWEEN
                    {"SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x BETWEEN 2 AND 5 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x", itemRows},
                    // the key on an INNER join after the RIGHT join
                    {"SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 1 RIGHT JOIN b ON a.x = b.k JOIN b b2 ON b2.id = b.id WHERE b2.k = o.x", itemRows},
                    // b row 23 holds a NULL x
                    {"SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x AND b.x = o.x", """
                            id\taid\tsid
                            1\tnull\t21
                            3\t11\t24
                            """},
                    // WHERE drops the rows that the RIGHT join NULL-extends
                    {"SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x AND a.x IS NOT NULL", """
                            id\taid\tsid
                            3\t11\t24
                            """},
                    {"SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 1 right join b ON a.x = b.k WHERE b.k = o.x", itemRows},
                    {"SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 1 FULL JOIN b ON a.x = b.k WHERE b.k = o.x", itemRows},
                    // a correlated sub-query before the RIGHT join reads o.x: for o.x = 1 every c row has
                    // id > o.x + 98, and a row 10 matches b rows 21 and 23; for o.x = 3 the c rows left
                    // match only a row 12, whose NULL x matches no b row
                    {"SELECT a.id aid, b.id sid FROM a JOIN (SELECT id, sid FROM c WHERE id > o.x + 98) cs ON cs.sid = a.id + 11 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x", """
                            id\taid\tsid
                            1\t10\t21
                            1\t10\t23
                            3\tnull\t24
                            """},
                    // the key on a column that only the layer above the joins has
                    {"SELECT aid, sid FROM (SELECT a.id aid, b.id sid, b.k bk FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 1 RIGHT JOIN b ON a.x = b.k WHERE a.id IS NOT NULL) WHERE bk = o.x", """
                            id\taid\tsid
                            3\t11\t24
                            """},
            };
            for (String[] c : cases) {
                final String sql = "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (" + c[0] + ") l ORDER BY 1, 2, 3";
                for (int i = 0; i < 2; i++) {
                    assertQuery(sql)
                            .noLeakCheck()
                            .expectSize()
                            .returns(c[1]);
                }
            }
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid - 20 = o.x RIGHT JOIN b ON a.x = b.k WHERE a.x = o.x AND a.id IS NOT NULL) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t0\t21
                            1\t0\t23
                            1\t10\t21
                            1\t10\t23
                            3\t11\t24
                            """);
        });
    }

    // The query of testLateralOuterConditionBeforeRightJoinKeyOnSlave() with o.x > $1, compiled once.
    // With $1 = 1 it returns the rows of that query. With a NULL $1, o.x > $1 holds for no outer row,
    // so the master side of the RIGHT join is empty for each, and DuckDB returns the b rows whose k
    // matches o.x, NULL-extended: 1 NULL 21, 1 NULL 23 and 3 NULL 24.
    @Test
    public void testLateralOuterConditionBeforeRightJoinKeyOnSlaveBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            final String sql = "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x > $1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x) l ORDER BY 1, 2, 3";
            final ObjList<BindVarTuple> binds = new ObjList<>();
            binds.add(BindVarTuple.ok("1", """
                    id\taid\tsid
                    1\tnull\t21
                    1\tnull\t23
                    3\t11\t24
                    """, bindVariableService -> bindVariableService.setInt(0, 1)));
            binds.add(BindVarTuple.ok("NULL", """
                    id\taid\tsid
                    1\tnull\t21
                    1\tnull\t23
                    3\tnull\t24
                    """, bindVariableService -> bindVariableService.setInt(0, Numbers.INT_NULL)));
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .assertBinds(binds);
            bindVariableService.clear();
        });
    }

    // The query of testLateralOuterConditionBeforeRightJoinKeyOnSlave() in a LEFT or CROSS JOIN
    // LATERAL, a CTE, a count(*) body, EXPLAIN and a view, with the rows of DuckDB, with IS NOT
    // DISTINCT FROM for QuestDB's =. LEFT JOIN LATERAL adds o row 2, for which the body returns no
    // row, and count(*) counts 2, 0 and 1 rows per outer row. With VARCHAR and SYMBOL keys,
    // o.x != '7' holds for every outer row, so the master side of the RIGHT join keeps a rows 10, 11
    // and 12, and a row 10 matches b rows 21 and 23 for o.x = '1', a row 11 b row 24 for o.x = '3';
    // DuckDB, with VARCHAR keys and IS DISTINCT FROM for QuestDB's !=, returns 1 10 21, 1 10 23 and
    // 3 11 24. On empty tables the query returns no rows.
    @Test
    public void testLateralOuterConditionBeforeRightJoinKeyOnSlaveContexts() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            final String body = "(SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x) l";
            final String rows = """
                    id\taid\tsid
                    1\tnull\t21
                    1\tnull\t23
                    3\t11\t24
                    """;
            assertQuery("SELECT o.id, l.aid, l.sid FROM o LEFT JOIN LATERAL " + body + " ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            1\tnull\t21
                            1\tnull\t23
                            2\tnull\tnull
                            3\t11\t24
                            """);
            final String[] queries = {
                    "SELECT o.id, l.aid, l.sid FROM o CROSS JOIN LATERAL " + body + " ORDER BY 1, 2, 3",
                    "WITH q AS (SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL " + body + ") SELECT * FROM q ORDER BY 1, 2, 3",
            };
            for (String sql : queries) {
                assertQuery(sql)
                        .noLeakCheck()
                        .expectSize()
                        .returns(rows);
            }
            assertQuery("SELECT o.id, l.n FROM o JOIN LATERAL (SELECT count(*) n FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x) l ORDER BY 1")
                    .noLeakCheck()
                    .returns("""
                            id\tn
                            1\t2
                            2\t0
                            3\t1
                            """);
            // WHERE reads only b, the side that the RIGHT join keeps whole, so any correct plan keeps a
            // right outer join. This pins that property of the plan, not the whole plan.
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL " + body)
                    .noLeakCheck()
                    .assertsPlanContaining("Hash Right Outer Join");
            execute("CREATE VIEW v AS (SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL " + body + ")");
            assertQuery("SELECT * FROM v ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns(rows);

            final String typedRows = """
                    id\taid\tsid
                    1\t10\t21
                    1\t10\t23
                    3\t11\t24
                    """;
            for (String type : new String[]{"VARCHAR", "SYMBOL"}) {
                execute("CREATE TABLE ot AS (SELECT id, x::" + type + " x FROM o)");
                execute("CREATE TABLE at AS (SELECT id, x::" + type + " x FROM a)");
                execute("CREATE TABLE bt AS (SELECT id, k::" + type + " k, x::" + type + " x FROM b)");
                assertQuery("SELECT o.id, l.aid, l.sid FROM ot o JOIN LATERAL (SELECT a.id aid, b.id sid FROM at a JOIN c ON c.sid = a.id + 11 AND o.x != '7' RIGHT JOIN bt b ON a.x = b.k WHERE b.k = o.x) l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .expectSize()
                        .returns(typedRows);
                execute("DROP TABLE ot");
                execute("DROP TABLE at");
                execute("DROP TABLE bt");
            }

            execute("TRUNCATE TABLE o");
            execute("TRUNCATE TABLE a");
            execute("TRUNCATE TABLE b");
            execute("TRUNCATE TABLE c");
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL " + body + " ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\taid\tsid\n");
        });
    }

    // c's ON clause reads o.x, and WHERE keys o.x on a column that is readable there: of a, of the
    // RIGHT join whose own ON clause reads it, or of a LEFT join. The rows come from DuckDB, with
    // IS NOT DISTINCT FROM for QuestDB's =, except for the RIGHT join with o.x in its own ON clause,
    // which DuckDB does not run: per outer row, o.x = 1 and the NULL o.x match no a row, so every b
    // row is NULL-extended, and b.k = o.x keeps b rows 21 and 23 for o.x = 1. With o2, WHERE keys
    // o2.x on a and o2.y on b, and c's ON clause reads o2.x only, so the query runs, and matches the
    // body run per outer row. Reading o2.y there instead, keyed on b, returns the same rows in DuckDB,
    // also when a later ON clause before the RIGHT join reads only o2.x, keyed on a. A view over the
    // query returns its rows, and so does the query with VARCHAR or SYMBOL keys.
    @Test
    public void testLateralOuterConditionBeforeRightJoinKeyOnMaster() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            execute("CREATE TABLE o2 (id INT, x INT, y INT)");
            execute("INSERT INTO o2 VALUES (1, 1, 1), (2, 3, 3), (3, 1, 5), (4, 3, 1)");
            final String masterKeyRows = """
                    id\taid\tsid
                    1\t10\t21
                    1\t10\t23
                    """;
            final String twoOuterColumns = "SELECT o2.id, l.aid, l.sid FROM o2 JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND c.id > o2.x + 98 RIGHT JOIN b ON a.x = b.k WHERE a.x = o2.x AND b.k = o2.y AND a.id IS NOT NULL) l";
            assertQuery(twoOuterColumns + " ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns(masterKeyRows);
            final String perOuterRow = "SELECT {I} id, a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND c.id > {X} + 98 RIGHT JOIN b ON a.x = b.k WHERE a.x = {X} AND b.k = {Y} AND a.id IS NOT NULL";
            assertSqlCursors(
                    "SELECT * FROM ("
                            + perOuterRow.replace("{I}", "1").replace("{X}", "1").replace("{Y}", "1") + " UNION ALL "
                            + perOuterRow.replace("{I}", "2").replace("{X}", "3").replace("{Y}", "3") + " UNION ALL "
                            + perOuterRow.replace("{I}", "3").replace("{X}", "1").replace("{Y}", "5") + " UNION ALL "
                            + perOuterRow.replace("{I}", "4").replace("{X}", "3").replace("{Y}", "1")
                            + ") ORDER BY 1, 2, 3",
                    twoOuterColumns + " ORDER BY 1, 2, 3"
            );
            assertQuery("SELECT o2.id, l.aid, l.sid FROM o2 JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND c.id > o2.y + 98 RIGHT JOIN b ON a.x = b.k WHERE a.x = o2.x AND b.k = o2.y AND a.id IS NOT NULL) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns(masterKeyRows);
            assertQuery("SELECT o2.id, l.aid, l.sid FROM o2 JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND c.id > o2.y + 98 JOIN c c2 ON c2.sid = c.sid AND o2.x > 0 RIGHT JOIN b ON a.x = b.k WHERE a.x = o2.x AND b.k = o2.y AND a.id IS NOT NULL) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns(masterKeyRows);

            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a RIGHT JOIN b ON a.x = b.k AND o.x > 1 WHERE b.k = o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\tnull\t21
                            1\tnull\t23
                            3\t11\t24
                            """);
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 + o.x LEFT JOIN b ON a.x = b.k WHERE b.k = o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns(masterKeyRows);

            execute("CREATE VIEW v AS (" + twoOuterColumns + ")");
            assertQuery("SELECT * FROM v ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns(masterKeyRows);

            final String typedRows = """
                    id\taid\tsid
                    1\t10\t21
                    1\t10\t23
                    3\t11\t24
                    """;
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x != 7 RIGHT JOIN b ON a.x = b.k WHERE a.x = o.x AND a.id IS NOT NULL) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns(typedRows);
            for (String type : new String[]{"VARCHAR", "SYMBOL"}) {
                execute("CREATE TABLE ot AS (SELECT id, x::" + type + " x FROM o)");
                execute("CREATE TABLE at AS (SELECT id, x::" + type + " x FROM a)");
                execute("CREATE TABLE bt AS (SELECT id, k::" + type + " k, x::" + type + " x FROM b)");
                assertQuery("SELECT o.id, l.aid, l.sid FROM ot o JOIN LATERAL (SELECT a.id aid, b.id sid FROM at a JOIN c ON c.sid = a.id + 11 AND o.x != '7' RIGHT JOIN bt b ON a.x = b.k WHERE a.x = o.x AND a.id IS NOT NULL) l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .expectSize()
                        .returns(typedRows);
                execute("DROP TABLE ot");
                execute("DROP TABLE at");
                execute("DROP TABLE bt");
            }
        });
    }

    // WHERE keys o.x on b2, which joins after c's ON clause reads o.x, but before the RIGHT join, as
    // an INNER or a LEFT join. DuckDB returns 1 10 21 and 1 10 23 for both.
    // Without a.id IS NOT NULL, which drops the rows that the RIGHT join NULL-extends, b2.k is NULL
    // on those rows. For the NULL o.x, o.x > 0 leaves the master side empty, so the RIGHT join
    // NULL-extends every b row, and QuestDB's = matches their NULL b2.k to the NULL o.x: DuckDB, with
    // IS NOT DISTINCT FROM, returns b rows 20 to 24 for o row 2 next to 1 10 21 and 1 10 23.
    @Test
    public void testLateralOuterConditionBeforeRightJoinKeyJustBeforeIt() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            for (String join : new String[]{"JOIN", "LEFT JOIN"}) {
                assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 0 " + join + " b b2 ON b2.id = c.sid RIGHT JOIN b ON a.x = b.k WHERE b2.k = o.x AND a.id IS NOT NULL) l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                id\taid\tsid
                                1\t10\t21
                                1\t10\t23
                                """);
            }
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 0 JOIN b b2 ON b2.id = c.sid RIGHT JOIN b ON a.x = b.k WHERE b2.k = o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t21
                            1\t10\t23
                            2\tnull\t20
                            2\tnull\t21
                            2\tnull\t22
                            2\tnull\t23
                            2\tnull\t24
                            """);
        });
    }

    // A UNION ALL body whose first branch is the body of
    // testLateralOuterConditionBeforeRightJoinKeyOnSlave(), and whose second branch matches a rows 10
    // and 0 to o.x = 1, a row 11 to o.x = 3 and a row 12 to the NULL o.x, as QuestDB's = matches NULL
    // to NULL. DuckDB, with IS NOT DISTINCT FROM for =, returns the rows of both branches, and sorts
    // the NULL aid first with NULLS FIRST, as QuestDB does. With the key on a the query returns the
    // rows of DuckDB as well.
    @Test
    public void testLateralOuterConditionBeforeRightJoinKeyOnSlaveInUnion() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x UNION ALL SELECT a.id, -1 FROM a WHERE a.x = o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\tnull\t21
                            1\tnull\t23
                            1\t0\t-1
                            1\t10\t-1
                            2\t12\t-1
                            3\t11\t-1
                            3\t11\t24
                            """);
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid - 20 = o.x RIGHT JOIN b ON a.x = b.k WHERE a.x = o.x AND a.id IS NOT NULL UNION ALL SELECT a.id, -1 FROM a WHERE a.x = o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t0\t-1
                            1\t0\t21
                            1\t0\t23
                            1\t10\t-1
                            1\t10\t21
                            1\t10\t23
                            2\t12\t-1
                            3\t11\t-1
                            3\t11\t24
                            """);
        });
    }

    // c's ON clause reads o.id, ahead of the RIGHT or FULL join, and WHERE keys o.x on b, the slave of
    // that join, or on a later table. Per outer row, o.id > 1 leaves the master side empty for o row
    // 1, a row 12 of the NULL o.x matches no b row, and a row 11 of o.x = 3 matches b row 24, so
    // DuckDB, with IS NOT DISTINCT FROM for =, returns 3 11 24 for each form of the key. On a.x < b.k,
    // a row 10 matches b row 24 instead. With c.sid = a.id + 10 + o.id, a row 10 of o row 1 matches b
    // rows 21 and 23. The FULL join keeps a row 12 of the NULL o.x, whose NULL b.k matches o.x. Each
    // query compiles twice. Without a filter that drops the rows that the RIGHT join NULL-extends,
    // o row 1 keeps b rows 21 and 23, whose k is 1. A b row with a NULL k then matches a row 12 of
    // the NULL o.x.
    @Test
    public void testLateralOuterConditionBeforeRightJoinKeyOnSlaveNullRejecting() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            final String item = "a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x AND a.id IS NOT NULL";
            final String noNullRejection = "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x) l ORDER BY 1, 2, 3";
            for (int i = 0; i < 2; i++) {
                assertQuery(noNullRejection)
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns("""
                                id\taid\tsid
                                1\tnull\t21
                                1\tnull\t23
                                3\t11\t24
                                """);
            }
            final String itemRows = """
                    id\taid\tsid
                    3\t11\t24
                    """;
            final String[][] cases = {
                    {item, itemRows},
                    {"a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k + 0 = o.x AND a.id IS NOT NULL", itemRows},
                    {"a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k >= o.x AND a.id IS NOT NULL", itemRows},
                    {"a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE coalesce(b.k, -1) = o.x AND a.id IS NOT NULL", itemRows},
                    {"a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k BETWEEN o.x AND o.x + 1 AND a.id IS NOT NULL", itemRows},
                    {"a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k IN (o.x, 99) AND a.id IS NOT NULL", itemRows},
                    // k only b has
                    {"a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE k = o.x AND a.id IS NOT NULL", itemRows},
                    {"a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x AND b.x = o.x AND a.id IS NOT NULL", itemRows},
                    // the key on an INNER or a LEFT join after the RIGHT join, or on a second RIGHT join
                    {"a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k JOIN b b2 ON b2.id = b.id WHERE b2.k = o.x AND a.id IS NOT NULL", itemRows},
                    {"a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k LEFT JOIN b b2 ON b2.id = b.id WHERE b2.k = o.x AND a.id IS NOT NULL", itemRows},
                    {"a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k RIGHT JOIN b b2 ON b2.id = b.id WHERE b2.k = o.x AND a.id IS NOT NULL", itemRows},
                    // two conjuncts that read no b column, where a.id > o.id + 8 drops a row 11 of o row 3,
                    // and a.id > o.id + 7 keeps it
                    {"a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x AND a.x >= o.x - 2 AND a.id > o.id + 8 AND a.id IS NOT NULL", "id\taid\tsid\n"},
                    {"a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x AND a.x >= o.x - 2 AND a.id > o.id + 7 AND a.id IS NOT NULL", itemRows},
                    // a conjunct that drops the NULL-extended rows and reads b and the outer row
                    {"a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x AND a.x < b.k + o.x", itemRows},
                    {"a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x < b.k WHERE b.k = o.x AND a.id IS NOT NULL", """
                            id\taid\tsid
                            3\t10\t24
                            """},
                    {"a JOIN c ON c.sid = a.id + 10 + o.id RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x AND a.id IS NOT NULL", """
                            id\taid\tsid
                            1\t10\t21
                            1\t10\t23
                            """},
                    {"a JOIN c ON c.sid = a.id + 11 AND o.id > 1 FULL JOIN b ON a.x = b.k WHERE b.k = o.x AND a.id IS NOT NULL", """
                            id\taid\tsid
                            2\t12\tnull
                            3\t11\t24
                            """},
            };
            for (String[] c : cases) {
                final String sql = "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM " + c[0] + ") l ORDER BY 1, 2, 3";
                for (int i = 0; i < 2; i++) {
                    assertQuery(sql)
                            .noLeakCheck()
                            .expectSize()
                            .returns(c[1]);
                }
            }
            assertLateralMatchesPerOuterRow("a JOIN c ON c.sid = a.id + 11 AND {I} > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = {X} AND a.id IS NOT NULL");
            assertLateralMatchesPerOuterRow("a JOIN c ON c.sid = a.id + 10 + {I} RIGHT JOIN b ON a.x = b.k WHERE b.k = {X} AND a.id IS NOT NULL");
            assertLateralMatchesPerOuterRow("a JOIN c ON c.sid = a.id + 11 AND {I} > 1 FULL JOIN b ON a.x = b.k WHERE b.k = {X} AND a.id IS NOT NULL");

            // a.x >= o.x - 2 reads no b column, so it stays in the ON clause of the outer-ref join and
            // filters ahead of the RIGHT join, and only b.k = o.x runs after it
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x AND a.x >= o.x - 2 AND a.id IS NOT NULL) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining(
                            "Filter filter: (a.id!=null and b.k=__qdb_outer_ref__0.__qdb_outer_ref__0_x)",
                            "Hash Right Outer Join Light",
                            "Filter filter: a.x>=__qdb_outer_ref__0.__qdb_outer_ref__0_x-2"
                    )
                    .returns(itemRows);

            execute("INSERT INTO b VALUES (25, NULL, NULL)");
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM " + item + ") l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            2\t12\t25
                            3\t11\t24
                            """);
            assertLateralMatchesPerOuterRow("a JOIN c ON c.sid = a.id + 11 AND {I} > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = {X} AND a.id IS NOT NULL");
        });
    }

    // The query of testLateralOuterConditionBeforeRightJoinKeyOnSlaveNullRejecting() with a.id >= $1,
    // which drops the rows that the RIGHT join NULL-extends while $1 is not NULL: with 5 it returns
    // the rows of DuckDB. With NULL, NULL >= NULL keeps those rows, b rows 21 and 23 of o row 1, and
    // 11 >= NULL drops a row 11.
    @Test
    public void testLateralOuterConditionBeforeRightJoinKeyOnSlaveNullRejectingBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            final String sql = "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x AND a.id >= $1) l ORDER BY 1, 2, 3";
            final String rows = "id\taid\tsid\n3\t11\t24\n";
            final ObjList<BindVarTuple> binds = new ObjList<>();
            binds.add(BindVarTuple.ok("5", rows, bindVariableService -> bindVariableService.setInt(0, 5)));
            binds.add(BindVarTuple.ok(
                    "NULL",
                    """
                            id\taid\tsid
                            1\tnull\t21
                            1\tnull\t23
                            """,
                    bindVariableService -> bindVariableService.setInt(0, Numbers.INT_NULL)
            ));
            binds.add(BindVarTuple.ok("5 again", rows, bindVariableService -> bindVariableService.setInt(0, 5)));
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .assertBinds(binds);
            bindVariableService.clear();
        });
    }

    // The query of testLateralOuterConditionBeforeRightJoinKeyOnSlaveNullRejecting() in a LEFT or
    // CROSS JOIN LATERAL, a lower-case CTE, a count(*) body, a wrapped body, a body with ORDER BY and
    // LIMIT, a LATERAL two levels below o, EXPLAIN and a view, with the rows of DuckDB. The plan runs
    // b.k = o.x after the RIGHT join, next to a.id IS NOT NULL. With VARCHAR and SYMBOL keys the
    // query returns the same rows, and on empty tables no rows.
    @Test
    public void testLateralOuterConditionBeforeRightJoinKeyOnSlaveNullRejectingContexts() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            final String body = "(SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x AND a.id IS NOT NULL) l";
            final String rows = """
                    id\taid\tsid
                    3\t11\t24
                    """;
            assertQuery("SELECT o.id, l.aid, l.sid FROM o LEFT JOIN LATERAL " + body + " ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            1\tnull\tnull
                            2\tnull\tnull
                            3\t11\t24
                            """);
            final String[] queries = {
                    "SELECT o.id, l.aid, l.sid FROM o CROSS JOIN LATERAL " + body + " ORDER BY 1, 2, 3",
                    "with q as (select o.id, l.aid, l.sid from o join lateral (select a.id aid, b.id sid from a join c on c.sid = a.id + 11 and o.id > 1 right join b on a.x = b.k where b.k = o.x and a.id is not null) l) select * from q order by 1, 2, 3",
                    "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT * FROM (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x AND a.id IS NOT NULL)) l ORDER BY 1, 2, 3",
                    "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT m.aid, m.sid FROM long_sequence(1) q JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x AND a.id IS NOT NULL) m) l ORDER BY 1, 2, 3",
            };
            for (String sql : queries) {
                assertQuery(sql)
                        .noLeakCheck()
                        .expectSize()
                        .returns(rows);
            }
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x AND a.id IS NOT NULL ORDER BY b.id LIMIT 1) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .returns(rows);
            assertQuery("SELECT o.id, l.n FROM o JOIN LATERAL (SELECT count(*) n FROM a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = o.x AND a.id IS NOT NULL) l ORDER BY 1")
                    .noLeakCheck()
                    .returns("""
                            id\tn
                            1\t0
                            2\t0
                            3\t1
                            """);
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL " + body)
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Hash Join
                                  condition: l.__qdb_outer_ref__0_id=o.id and l.__qdb_outer_ref__0_x=o.x
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: o
                                    Hash
                                        SelectedRecord
                                            Filter filter: (a.id!=null and b.k=__qdb_outer_ref__0.__qdb_outer_ref__0_x)
                                                Hash Right Outer Join Light
                                                  condition: b.k=a.x
                                                    Filter filter: c.sid=a.id+11
                                                        Cross Join
                                                            Cross Join
                                                                Async JIT Filter workers: 1
                                                                  filter: id!=null
                                                                    PageFrame
                                                                        Row forward scan
                                                                        Frame forward scan on: a
                                                                Async JIT Group By workers: 1
                                                                  keys: [__qdb_outer_ref__0_id,__qdb_outer_ref__0_x]
                                                                  filter: 1<id
                                                                    PageFrame
                                                                        Row forward scan
                                                                        Frame forward scan on: o
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: c
                                                    Hash
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: b
                            """);
            execute("CREATE VIEW v AS (SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL " + body + ")");
            assertQuery("SELECT * FROM v ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns(rows);

            for (String type : new String[]{"VARCHAR", "SYMBOL"}) {
                execute("CREATE TABLE ot AS (SELECT id, x::" + type + " x FROM o)");
                execute("CREATE TABLE at AS (SELECT id, x::" + type + " x FROM a)");
                execute("CREATE TABLE bt AS (SELECT id, k::" + type + " k, x::" + type + " x FROM b)");
                assertQuery("SELECT o.id, l.aid, l.sid FROM ot o JOIN LATERAL (SELECT a.id aid, b.id sid FROM at a JOIN c ON c.sid = a.id + 11 AND o.id > 1 RIGHT JOIN bt b ON a.x = b.k WHERE b.k = o.x AND a.id IS NOT NULL) l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .expectSize()
                        .returns(rows);
                execute("DROP TABLE ot");
                execute("DROP TABLE at");
                execute("DROP TABLE bt");
            }

            execute("TRUNCATE TABLE a");
            execute("TRUNCATE TABLE b");
            execute("TRUNCATE TABLE c");
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL " + body + " ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\taid\tsid\n");
        });
    }

    // o.ts is TIMESTAMP and s.ts TIMESTAMP_NS. c's ON clause reads o.ts, which WHERE keys on s, the
    // slave of the RIGHT join, and a.ts < o.ts + 1 adds a microsecond to o.ts, where s.ts + 1 would add
    // a nanosecond, so the rewriter keeps the outer-ref join ahead of the RIGHT join; see
    // testLateralWhereKeyAndCorrelatedComparisonRetypedKey().
    // Per outer row, a.ts < o.ts + 1 keeps a row 10 (1.5us) for o row 1 (1us), which matches s row 20,
    // and a rows 10 and 11 (5.5us) for o row 2 (5us), of which a row 11 matches s row 21, at 5us. On
    // a nanosecond, a.ts < o.ts + 1 would keep neither of these rows. The plan runs s.ts = o.ts after
    // the RIGHT join and a.ts < o.ts + 1 on the outer-ref join ahead of it.
    @Test
    public void testLateralOuterConditionBeforeRightJoinKeyOnSlaveRetypedKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, ts TIMESTAMP)");
            execute("CREATE TABLE a (id INT, x INT, ts TIMESTAMP_NS)");
            execute("CREATE TABLE c (id INT, sid INT)");
            execute("CREATE TABLE s (id INT, k INT, ts TIMESTAMP_NS)");
            execute("INSERT INTO o VALUES (1, '2024-01-01T00:00:00.000001Z'), (2, '2024-01-01T00:00:00.000005Z')");
            execute("INSERT INTO a VALUES (10, 1, '2024-01-01T00:00:00.000001500Z'), (11, 2, '2024-01-01T00:00:00.000005500Z')");
            execute("INSERT INTO c VALUES (100, 21), (101, 22)");
            execute("INSERT INTO s VALUES (20, 1, '2024-01-01T00:00:00.000001000Z'), (21, 2, '2024-01-01T00:00:00.000005000Z')");
            final String body = "SELECT a.id aid, s.id sid FROM a JOIN c ON c.sid = a.id + 11 AND {T} > '2024-01-01'::timestamp RIGHT JOIN s ON a.x = s.k WHERE s.ts = {T} AND a.ts < {T} + 1 AND a.id IS NOT NULL";
            final String sql = "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (" + body.replace("{T}", "o.ts") + ") l ORDER BY 1, 2, 3";
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining(
                            "Filter filter: (a.id!=null and __qdb_outer_ref__0.__qdb_outer_ref__0_ts=s.ts)",
                            "Hash Right Outer Join Light",
                            "Filter filter: a.ts<__qdb_outer_ref__0.__qdb_outer_ref__0_ts+1"
                    )
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            2\t11\t21
                            """);
            assertSqlCursors(
                    "SELECT * FROM (SELECT 1 id, aid, sid FROM (" + body.replace("{T}", "'2024-01-01T00:00:00.000001Z'::timestamp")
                            + ") UNION ALL SELECT 2 id, aid, sid FROM (" + body.replace("{T}", "'2024-01-01T00:00:00.000005Z'::timestamp")
                            + ")) ORDER BY 1, 2, 3",
                    sql
            );
        });
    }

    // A UNION ALL, EXCEPT or INTERSECT body keeps the outer-ref join, and the body of
    // testLateralOuterConditionBeforeRightJoinKeyOnSlaveInUnion() with a.id IS NOT NULL drops the
    // rows that the RIGHT join NULL-extends. The rewriter moves b.k = o.x after the RIGHT join, and
    // the query returns the rows of DuckDB in a JOIN or LEFT JOIN LATERAL, with the RIGHT join in the
    // first or the second branch, and the body run once per outer row.
    @Test
    public void testLateralOuterConditionBeforeRightJoinKeyOnSlaveInUnionNullRejecting() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            final String rightBranch = "SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 11 AND {X} > 1 RIGHT JOIN b ON a.x = b.k WHERE b.k = {X} AND a.id IS NOT NULL";
            final String plainBranch = "SELECT a.id aid, -1 sid FROM a WHERE a.x = {X}";
            final String unionRows = """
                    id\taid\tsid
                    1\t0\t-1
                    1\t10\t-1
                    2\t12\t-1
                    3\t11\t-1
                    3\t11\t24
                    """;
            final String union = rightBranch + " UNION ALL " + plainBranch;
            final String[] bodies = {union, plainBranch + " UNION ALL " + rightBranch};
            for (String body : bodies) {
                final String sql = "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (" + body.replace("{X}", "o.x") + ") l ORDER BY 1, 2, 3";
                for (int i = 0; i < 2; i++) {
                    assertQuery(sql)
                            .noLeakCheck()
                            .expectSize()
                            .returns(unionRows);
                }
            }
            assertQuery("SELECT o.id, l.aid, l.sid FROM o LEFT JOIN LATERAL (" + union.replace("{X}", "o.x") + ") l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .returns(unionRows);
            assertSqlCursors(
                    "SELECT * FROM (SELECT 1 id, aid, sid FROM (" + union.replace("{X}", "1")
                            + ") UNION ALL SELECT 2 id, aid, sid FROM (" + union.replace("{X}", "NULL::int")
                            + ") UNION ALL SELECT 3 id, aid, sid FROM (" + union.replace("{X}", "3")
                            + ")) ORDER BY 1, 2, 3",
                    "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (" + union.replace("{X}", "o.x") + ") l ORDER BY 1, 2, 3"
            );
            final String itemRows = """
                    id\taid\tsid
                    3\t11\t24
                    """;
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (" + (rightBranch + " EXCEPT " + plainBranch).replace("{X}", "o.x") + ") l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns(itemRows);
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (" + (rightBranch + " INTERSECT SELECT a.id, 24 FROM a WHERE a.x = {X}").replace("{X}", "o.x") + ") l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns(itemRows);
        });
    }

    // c's ON clause reads o.x, and WHERE keys o.x on b, which a SPLICE, ASOF or INNER join joins after
    // c. The SPLICE join needs a master with random access, which no join provides, so the joins
    // written before it run after it, here and per outer row alike, and the rewritten ON clause reads
    // b there. Each query matches its body run once per outer row, on the tables of
    // testLateralOuterOnlyConditionBeforeSpliceJoin() and on tables where SPLICE and ASOF pick other
    // b rows per a row.
    @Test
    public void testLateralOuterConditionBeforeSpliceJoinKeyOnSlave() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("CREATE TABLE a (id INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE c (id INT, sid INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE b (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO o VALUES (1, 1), (2, NULL), (3, 3)");
            execute("INSERT INTO a VALUES (10, 1, 1::timestamp), (11, 3, 2::timestamp), (12, NULL, 3::timestamp), (0, 1, 4::timestamp)");
            execute("INSERT INTO c VALUES (100, 21, 1::timestamp), (101, 22, 2::timestamp), (102, 23, 3::timestamp), (103, 11, 4::timestamp)");
            execute("INSERT INTO b VALUES (20, 5, 1::timestamp), (21, 1, 2::timestamp), (24, 3, 5::timestamp)");
            assertLateralMatchesPerOuterRow("a JOIN c ON c.sid = a.id + 11 AND {X} > 1 SPLICE JOIN b WHERE b.k = {X}");
            assertLateralMatchesPerOuterRow("a JOIN c ON c.sid = a.id + 11 + {X} SPLICE JOIN b WHERE b.k = {X}");

            execute("TRUNCATE TABLE a");
            execute("TRUNCATE TABLE b");
            execute("TRUNCATE TABLE c");
            createSpliceKeyRows();
            assertLateralMatchesPerOuterRow("a JOIN c ON c.sid = a.id + 0 AND {X} > 1 SPLICE JOIN b WHERE b.k = {X}");
            assertLateralMatchesPerOuterRow("a JOIN c ON c.sid = a.id + 0 AND {X} > 1 SPLICE JOIN b JOIN c d ON d.id = b.id + 80 WHERE d.sid = {X}");
            assertLateralMatchesPerOuterRow("a JOIN c ON c.sid = a.id + 0 AND {X} > 1 ASOF JOIN b WHERE b.k = {X}");
        });
    }

    // A RIGHT join after the SPLICE join: c's ON clause reads o.x, and WHERE keys o.x on e, which the
    // RIGHT join joins. The SPLICE join needs a master with random access, which the join of a and c
    // does not provide, so the body fails at the SPLICE join when it runs once per outer row with o.x
    // as a constant, and so does the query.
    @Test
    public void testLateralOuterConditionBeforeSpliceThenRightJoinNoRandomAccess() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("CREATE TABLE a (id INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE c (id INT, sid INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE b (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO o VALUES (1, 1), (2, NULL), (3, 3)");
            createSpliceKeyRows();
            final String sql = "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT a.id aid, b.id sid FROM a JOIN c ON c.sid = a.id + 0 AND o.x > 1 SPLICE JOIN b RIGHT JOIN c e ON e.sid = b.k WHERE e.sid = o.x) l";
            assertQuery(sql)
                    .noLeakCheck()
                    .fails(sql.indexOf("SPLICE"), "left side of splice join doesn't support random access");
        });
    }

    @Test
    public void testLateralWhereDuplicatedKeyAndComparison() throws Exception {
        assertKeyAndComparison(
                false,
                "SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.x = o.x AND a.x = o.x AND a.id > o.x) l ORDER BY 1, 2",
                """
                        id\taid
                        1\t10
                        3\t11
                        """
        );
    }

    // The rewriter equates o.x to a.x and keeps a.id = o.x + 9, which is no plain column equality, as a
    // condition of the lateral join, which the hash join evaluates on the key of the body that equals o.x.
    @Test
    public void testLateralWhereKeyAndComputedEquality() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.x = o.x AND a.id = o.x + 9) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("Hash Join Light", "filter: l.aid=l.__qdb_outer_ref__0_x+9")
                    .withPlanNotContaining("Filter filter")
                    .returns("""
                            id\taid
                            1\t10
                            """);
        });
    }

    // The rewriter removes the outer-ref join, which a GROUP BY over o would feed, and keeps a.id > o.x as
    // a condition of the lateral join. The hash join evaluates it on the key of the body, which equals o.x,
    // once per row of a when it hashes o, and the scan of a stays unfiltered.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparison() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.x = o.x AND a.id > o.x) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("Hash Join Light", "filter: l.__qdb_outer_ref__0_x<l.aid", "Frame forward scan on: a")
                    .withPlanNotContaining("Group By", "Filter filter", "Async JIT Filter")
                    .returns("""
                            id\taid
                            1\t10
                            3\t11
                            """);
        });
    }

    // A bind variable in the comparison: $1 = -2 keeps a row 0 for o.x = 1, and a NULL $1 makes the
    // comparison false for every row. One compiled query serves each value.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            final ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok(
                    "zero",
                    """
                            id\taid
                            1\t10
                            3\t11
                            """,
                    bindVariableService -> bindVariableService.setInt(0, 0)
            ));
            cases.add(BindVarTuple.ok(
                    "negative",
                    """
                            id\taid
                            1\t0
                            1\t10
                            3\t11
                            """,
                    bindVariableService -> bindVariableService.setInt(0, -2)
            ));
            cases.add(BindVarTuple.ok(
                    "NULL",
                    "id\taid\n",
                    bindVariableService -> bindVariableService.setInt(0, Numbers.INT_NULL)
            ));
            assertQuery("SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.x = o.x AND a.id > o.x + $1) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .assertBinds(cases);
        });
    }

    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonCount() throws Exception {
        assertKeyAndComparison(
                false,
                "SELECT o.id, l.n FROM o JOIN LATERAL (SELECT count(*) n FROM a WHERE a.x = o.x AND a.id > o.x) l ORDER BY 1",
                """
                        id\tn
                        1\t1
                        2\t0
                        3\t1
                        """
        );
    }

    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonDistinct() throws Exception {
        assertKeyAndComparison(
                false,
                "SELECT o.id, l.ax FROM o JOIN LATERAL (SELECT DISTINCT a.x ax FROM a WHERE a.x = o.x AND a.id > o.x) l ORDER BY 1, 2",
                """
                        id\tax
                        1\t1
                        3\t3
                        """
        );
    }

    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonGroupBy() throws Exception {
        assertKeyAndComparison(
                false,
                "SELECT o.id, l.ax, l.n FROM o JOIN LATERAL (SELECT a.x ax, count(*) n FROM a WHERE a.x = o.x AND a.id > o.x GROUP BY a.x) l ORDER BY 1, 2",
                """
                        id\tax\tn
                        1\t1\t1
                        3\t3\t1
                        """
        );
    }

    // a.id > o.x on the inner table of an INNER join; per outer row, b rows 21 and 23 match a rows 10
    // and 0 for o.x = 1, and only row 23 has an id above 21. The join in the body makes the lateral join
    // a full-fat hash join, which hashes the body, so the comparison stays after it, on o.x, and the
    // cursor does not know its size.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonInnerJoinSlave() throws Exception {
        assertKeyAndComparison(
                false,
                "SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (SELECT a.id aid, b.id bid FROM a JOIN b ON a.x = b.k WHERE a.x = o.x AND b.id > o.x + 20) l ORDER BY 1, 2, 3",
                """
                        id\taid\tbid
                        1\t0\t23
                        1\t10\t23
                        3\t11\t24
                        """
        );
    }

    // The WHERE key is on the outer layer over a sub-query of a.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonKeyOnOuterLayer() throws Exception {
        assertKeyAndComparison(
                false,
                "SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT t.id aid FROM (SELECT id, x FROM a) t WHERE t.x = o.x AND t.id > o.x) l ORDER BY 1, 2",
                """
                        id\taid
                        1\t10
                        3\t11
                        """
        );
    }

    // b.id > o.x on the slave of a LEFT join: for o.x = 1, b rows 21 and 23 match a rows 10 and 0;
    // the NULL o.x leaves a row 12, which matches no b row, and b.id > NULL drops it. As in
    // testLateralWhereKeyAndCorrelatedComparisonInnerJoinSlave(), the comparison stays after the full-fat
    // lateral join.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonLaterLeftJoinSlave() throws Exception {
        assertKeyAndComparison(
                false,
                "SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (SELECT a.id aid, b.id bid FROM a LEFT JOIN b ON a.x = b.k WHERE a.x = o.x AND b.id > o.x) l ORDER BY 1, 2, 3",
                """
                        id\taid\tbid
                        1\t0\t21
                        1\t0\t23
                        1\t10\t21
                        1\t10\t23
                        3\t11\t24
                        """
        );
    }

    // The LATEST ON body filters the rows before it picks the latest per s: for k = 1, v > 1 drops
    // row (p, 0) at 02:00 and leaves (p, 5) at 01:00; the NULL k matches row (p, 9), which v > NULL
    // drops; for k = 2, only (q, 8) passes v > 2.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonLatestOn() throws Exception {
        assertMemoryLeak(() -> {
            createLatestOnKeyTables();
            assertQuery("SELECT o.id, l.s, l.v FROM o JOIN LATERAL (SELECT s, v, ts FROM t WHERE k = o.k AND v > o.k LATEST ON ts PARTITION BY s) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .returns("""
                            id\ts\tv
                            1\tp\t5
                            1\tq\t7
                            2\tp\t5
                            2\tq\t7
                            4\tq\t8
                            """);
        });
    }

    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonLeftLateral() throws Exception {
        assertKeyAndComparison(
                false,
                "SELECT o.id, l.aid FROM o LEFT JOIN LATERAL (SELECT a.id aid FROM a WHERE a.x = o.x AND a.id > o.x) l ORDER BY 1, 2",
                """
                        id\taid
                        1\t10
                        2\tnull
                        3\t11
                        """
        );
    }

    // With JIT off, the hash join evaluates the comparison on the key of the body as well, see
    // testLateralWhereKeyAndCorrelatedComparison(): no scan filter holds it.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonJitDisabled() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            final int callerJitMode = sqlExecutionContext.getJitMode();
            try {
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                assertQuery("SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.x = o.x AND a.id > o.x) l ORDER BY 1, 2")
                        .noLeakCheck()
                        .withPlanContaining("Hash Join Light", "filter: l.__qdb_outer_ref__0_x<l.aid")
                        .withPlanNotContaining("Filter filter", "Async Filter")
                        .returns("""
                                id\taid
                                1\t10
                                3\t11
                                """);
            } finally {
                sqlExecutionContext.setJitMode(callerJitMode);
            }
        });
    }

    // o.ts is TIMESTAMP and a.ts TIMESTAMP_NS, which compare equal at the same instant, but + 1 adds
    // a microsecond to the first and a nanosecond to the second. Per outer row, o.ts + 1 is 2us for
    // o row 1 and 6us for o row 2, so a.y < o.ts + 1 keeps a row 10 (y 1.5us), and a.y > o.ts + 1
    // and a.y BETWEEN o.ts + 1 AND o.ts + 10 keep a row 11 (y 7us). On a.ts + 1, 1.001us and
    // 5.001us, each would change. The comparison stays after the lateral join, on the TIMESTAMP outer
    // column: the hash join moves a condition onto the key of the body only when both keys have one
    // type.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonRetypedKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, ts TIMESTAMP)");
            execute("CREATE TABLE a (id INT, ts TIMESTAMP_NS, y TIMESTAMP_NS)");
            execute("INSERT INTO o VALUES (1, '2024-01-01T00:00:00.000001Z'), (2, '2024-01-01T00:00:00.000005Z')");
            execute("""
                    INSERT INTO a VALUES
                    (10, '2024-01-01T00:00:00.000001000Z', '2024-01-01T00:00:00.000001500Z'),
                    (11, '2024-01-01T00:00:00.000005000Z', '2024-01-01T00:00:00.000007000Z')
                    """);
            assertQuery("SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.ts = o.ts AND a.y < o.ts + 1) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .returns("""
                            id\taid
                            1\t10
                            """);
            assertQuery("SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.ts = o.ts AND a.y BETWEEN o.ts + 1 AND o.ts + 10) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .returns("""
                            id\taid
                            2\t11
                            """);
            assertQuery("SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.ts = o.ts AND a.y > o.ts + 1) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: o.ts+1<l.y", "Hash Join Light")
                    .returns("""
                            id\taid
                            2\t11
                            """);
        });
    }

    // testLateralWhereKeyAndCorrelatedComparisonRetypedKey() over a computed master column: the
    // sub-query casts a.ts, which is TIMESTAMP_NS already, to TIMESTAMP_NS. o.ts is TIMESTAMP, so
    // o.ts + 1 adds a microsecond per outer row, as the body run once per outer row with o.ts as a
    // TIMESTAMP literal shows: a.y < o.ts + 1 keeps a row 10 (1.5us < 2us) for o row 1, and
    // a.y > o.ts + 1 and a.y BETWEEN o.ts + 1 AND o.ts + 10 keep a row 11 (7us, against 6us and
    // 15us) for o row 2. The NULL o.ts of o row 3 matches no row. On a.ts + 1, which adds a
    // nanosecond, each comparison keeps other rows.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonRetypedKeyComputedMaster() throws Exception {
        assertMemoryLeak(() -> {
            createRetypedKeyComparisonTables();
            final String[][] cases = {
                    {"a.y < o.ts + 1", "id\taid\n1\t10\n"},
                    {"a.y > o.ts + 1", "id\taid\n2\t11\n"},
                    {"a.y BETWEEN o.ts + 1 AND o.ts + 10", "id\taid\n2\t11\n"},
            };
            for (String[] c : cases) {
                assertQuery("SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM (SELECT id, ts::timestamp_ns ts, y FROM a) a WHERE a.ts = o.ts AND " + c[0] + ") l ORDER BY 1, 2")
                        .noLeakCheck()
                        .returns(c[1]);
            }
        });
    }

    // The comparisons of testLateralWhereKeyAndCorrelatedComparisonRetypedKeyComputedMaster() on a
    // UNION ALL of a, whose second branch is empty, joined to t by id. Per outer row, with o.ts as a
    // TIMESTAMP literal, a.y < o.ts + 1 keeps a row 10 for o row 1, and a.y > o.ts + 1 and
    // a.y BETWEEN o.ts + 1 AND o.ts + 10 keep a row 11 for o row 2, each joined to the t row of its id.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonRetypedKeyJoinedUnion() throws Exception {
        assertMemoryLeak(() -> {
            createRetypedKeyComparisonTables();
            final String[][] cases = {
                    {"a.y < o.ts + 1", "id\taid\ttid\n1\t10\t10\n"},
                    {"a.y > o.ts + 1", "id\taid\ttid\n2\t11\t11\n"},
                    {"a.y BETWEEN o.ts + 1 AND o.ts + 10", "id\taid\ttid\n2\t11\t11\n"},
            };
            for (String[] c : cases) {
                assertQuery("SELECT o.id, l.aid, l.tid FROM o JOIN LATERAL (SELECT a.id aid, t.id tid FROM t JOIN (SELECT * FROM a UNION ALL SELECT * FROM a WHERE id < 0) a ON a.id = t.id WHERE a.ts = o.ts AND " + c[0] + ") l ORDER BY 1, 2, 3")
                        .noLeakCheck()
                        .returns(c[1]);
            }
        });
    }

    // The outer column is computed: o.ts cast to TIMESTAMP_NS, keyed to the TIMESTAMP a2.ts, so
    // o.ts + 1 adds a nanosecond. Per outer row, with o.ts as a TIMESTAMP_NS literal,
    // a.y <= o.ts + 1 keeps a2 row 13 (5us <= 5.000001us) for o row 2 but not a2 row 12
    // (2us > 1.000001us) for o row 1, and a.y > o.ts + 1 keeps a2 row 12 only. On a.ts + 1, which
    // adds a microsecond, each comparison keeps other rows.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonRetypedKeyComputedOuter() throws Exception {
        assertMemoryLeak(() -> {
            createRetypedKeyComparisonTables();
            final String[][] cases = {
                    {"a.y <= o.ts + 1", "id\taid\n2\t13\n"},
                    {"a.y > o.ts + 1", "id\taid\n1\t12\n"},
            };
            for (String[] c : cases) {
                assertQuery("SELECT o.id, l.aid FROM (SELECT id, ts::timestamp_ns ts FROM o) o JOIN LATERAL (SELECT a.id aid FROM a2 a WHERE a.ts = o.ts AND " + c[0] + ") l ORDER BY 1, 2")
                        .noLeakCheck()
                        .returns(c[1]);
            }
        });
    }

    // a has more rows than o, so the hash join of the lateral join hashes o and evaluates the comparison once
    // per row of a whose x some row of o has: a rows 10, 11 and 12, not once per row of the join, seven. o rows
    // 1, 2 and 3 share x = 1, which a rows 10 and 11 pass (10us and 11us > 1us); a row 12 fails 1us > 2us.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonOncePerInnerRow() throws Exception {
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("CREATE TABLE a (id INT, x INT, ts TIMESTAMP)");
            execute("INSERT INTO o VALUES (1, 1), (2, 1), (3, 1), (4, 2)");
            execute("""
                    INSERT INTO a VALUES
                    (10, 1, 10::TIMESTAMP), (11, 1, 11::TIMESTAMP), (12, 2, 1::TIMESTAMP),
                    (13, 3, 13::TIMESTAMP), (14, 3, 14::TIMESTAMP), (15, 3, 15::TIMESTAMP)
                    """);
            final String sql = "SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.x = o.x AND test_timestamp_counter(a.ts) > o.x::TIMESTAMP) l";
            TestTimestampCounterFactory.COUNTER.set(0);
            long rows = 0;
            try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                    rows++;
                }
            }
            Assert.assertEquals(6, rows);
            Assert.assertEquals(3, TestTimestampCounterFactory.COUNTER.get());
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlanContaining("Hash Join Light", "filter: l.__qdb_outer_ref__0_x::timestamp<test_timestamp_counter");
        });
    }

    // Per outer row, ORDER BY a.id LIMIT 1 picks a row 10, not row 0, for o.x = 1, as a.id > o.x
    // drops row 0 first.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonOrderByLimit() throws Exception {
        assertKeyAndComparison(
                false,
                "SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.x = o.x AND a.id > o.x ORDER BY a.id LIMIT 1) l ORDER BY 1, 2",
                """
                        id\taid
                        1\t10
                        3\t11
                        """
        );
    }

    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonOuterFirst() throws Exception {
        assertKeyAndComparison(
                false,
                "SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE o.x = a.x AND o.x < a.id) l ORDER BY 1, 2",
                """
                        id\taid
                        1\t10
                        3\t11
                        """
        );
    }

    // b.id > o.x on the slave of a RIGHT join to a table: the rows that the join NULL-extends have a
    // NULL a.x, which WHERE a.x = o.x keeps for the NULL o.x only, and b.id > NULL drops them. As in
    // testLateralWhereKeyAndCorrelatedComparisonInnerJoinSlave(), the comparison stays after the full-fat
    // lateral join.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonRightJoinTableSlave() throws Exception {
        assertKeyAndComparison(
                false,
                "SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (SELECT a.id aid, b.id bid FROM a RIGHT JOIN b ON a.x = b.k WHERE a.x = o.x AND b.id > o.x) l ORDER BY 1, 2, 3",
                """
                        id\taid\tbid
                        1\t0\t21
                        1\t0\t23
                        1\t10\t21
                        1\t10\t23
                        3\t11\t24
                        """
        );
    }

    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonRowNumber() throws Exception {
        assertKeyAndComparison(
                false,
                "SELECT o.id, l.aid, l.rn FROM o JOIN LATERAL (SELECT a.id aid, row_number() OVER (ORDER BY a.id) rn FROM a WHERE a.x = o.x AND a.id > o.x) l ORDER BY 1, 2",
                """
                        id\taid\trn
                        1\t10\t1
                        3\t11\t1
                        """
        );
    }

    // The SAMPLE BY body filters the rows before it sums them per hour; the reference runs the body
    // once per outer row, with its o.k as a constant.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createLatestOnKeyTables();
            assertSqlCursors(
                    """
                            SELECT * FROM (
                                SELECT 1 id, ts, total FROM (SELECT t.ts, sum(t.v) total FROM t WHERE t.k = 1 AND t.v > 1 SAMPLE BY 1h)
                                UNION ALL
                                SELECT 2 id, ts, total FROM (SELECT t.ts, sum(t.v) total FROM t WHERE t.k = 1 AND t.v > 1 SAMPLE BY 1h)
                                UNION ALL
                                SELECT 3 id, ts, total FROM (SELECT t.ts, sum(t.v) total FROM t WHERE t.k = NULL AND t.v > NULL SAMPLE BY 1h)
                                UNION ALL
                                SELECT 4 id, ts, total FROM (SELECT t.ts, sum(t.v) total FROM t WHERE t.k = 2 AND t.v > 2 SAMPLE BY 1h)
                            ) ORDER BY 1, 2""",
                    "SELECT o.id, l.ts, l.total FROM o JOIN LATERAL (SELECT t.ts, sum(t.v) total FROM t WHERE t.k = o.k AND t.v > o.k SAMPLE BY 1h) l ORDER BY 1, 2"
            );
            assertQuery("SELECT o.id, l.ts, l.total FROM o JOIN LATERAL (SELECT t.ts, sum(t.v) total FROM t WHERE t.k = o.k AND t.v > o.k SAMPLE BY 1h) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .returns("""
                            id\tts\ttotal
                            1\t2024-01-01T01:00:00.000000Z\t5
                            1\t2024-01-01T04:00:00.000000Z\t7
                            2\t2024-01-01T01:00:00.000000Z\t5
                            2\t2024-01-01T04:00:00.000000Z\t7
                            4\t2024-01-01T05:00:00.000000Z\t8
                            """);
        });
    }

    // Each type of key, with a second column k2 of the same type compared with the outer key, so that
    // the comparison reads only the outer column that the WHERE key maps to a.k. = and != treat NULL
    // as a value. Per outer row, the a rows whose k matches and whose k2 differs from it: 5 and 6 for
    // v1, 1 for v2 and 2 for NULL. The rewriter removes the outer-ref join, which a GROUP BY over o
    // would feed, and runs a.k2 != a.k on the scan of a.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonSecondKeyTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String[][] types = {
                    {"SYMBOL", "'p'", "'q'"},
                    {"VARCHAR", "'p'", "'q'"},
                    {"STRING", "'p'", "'q'"},
                    {"TIMESTAMP", "'2024-01-01T00:00:00.000000Z'", "'2024-01-02T00:00:00.000000Z'"},
                    {"DOUBLE", "1.5", "2.5"},
                    {"UUID", "'11111111-1111-1111-1111-111111111111'", "'22222222-2222-2222-2222-222222222222'"},
                    {"IPv4", "'1.1.1.1'", "'2.2.2.2'"},
                    {"LONG256", "'0x01'", "'0x02'"},
            };
            for (String[] type : types) {
                final String v1 = type[1];
                final String v2 = type[2];
                execute("CREATE TABLE o (id INT, k " + type[0] + ")");
                execute("CREATE TABLE a (id INT, k " + type[0] + ", k2 " + type[0] + ")");
                execute("INSERT INTO o VALUES (1, " + v1 + "), (2, " + v2 + "), (3, NULL)");
                execute("INSERT INTO a VALUES "
                        + "(0, " + v1 + ", " + v1 + "), (5, " + v1 + ", " + v2 + "), (6, " + v1 + ", NULL), "
                        + "(1, " + v2 + ", " + v1 + "), (7, " + v2 + ", " + v2 + "), "
                        + "(2, NULL, " + v1 + "), (8, NULL, NULL)");
                assertQuery("SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.k = o.k AND a.k2 != o.k) l ORDER BY 1, 2")
                        .noLeakCheck()
                        .withPlanNotContaining("Group By")
                        .returns("""
                                id\taid
                                1\t5
                                1\t6
                                2\t1
                                3\t2
                                """);
                execute("DROP TABLE o");
                execute("DROP TABLE a");
            }
        });
    }

    // Each type of key, with values v1, v2 and NULL; = matches NULL to NULL. Per outer row, a.id >
    // o.id keeps the a row with the larger id of each key: 5 for v1, 6 for v2 and 7 for NULL.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String[][] types = {
                    {"SYMBOL", "'p'", "'q'"},
                    {"VARCHAR", "'p'", "'q'"},
                    {"STRING", "'p'", "'q'"},
                    {"TIMESTAMP", "'2024-01-01T00:00:00.000000Z'", "'2024-01-02T00:00:00.000000Z'"},
                    {"DOUBLE", "1.5", "2.5"},
                    {"UUID", "'11111111-1111-1111-1111-111111111111'", "'22222222-2222-2222-2222-222222222222'"},
                    {"IPv4", "'1.1.1.1'", "'2.2.2.2'"},
                    {"LONG256", "'0x01'", "'0x02'"},
            };
            for (String[] type : types) {
                execute("CREATE TABLE o (id INT, k " + type[0] + ")");
                execute("CREATE TABLE a (id INT, k " + type[0] + ")");
                execute("INSERT INTO o VALUES (1, " + type[1] + "), (2, " + type[2] + "), (3, NULL)");
                execute("INSERT INTO a VALUES (0, " + type[1] + "), (5, " + type[1] + "), (1, " + type[2] + "), (6, " + type[2] + "), (2, NULL), (7, NULL)");
                assertQuery("SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.k = o.k AND a.id > o.id) l ORDER BY 1, 2")
                        .noLeakCheck()
                        .returns("""
                                id\taid
                                1\t5
                                2\t6
                                3\t7
                                """);
                execute("DROP TABLE o");
                execute("DROP TABLE a");
            }
        });
    }

    // The key is a computed column on one side, whose type only the code generator knows: of the
    // sub-query over a, or of the outer sub-query over o. The rewriter removes the outer-ref join,
    // which a GROUP BY over o would feed, and runs the comparison after the lateral join, on o.x.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonUnknownKeyType() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT t.id aid FROM (SELECT id, x + 0 x FROM a) t WHERE t.x = o.x AND t.id > o.x) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanNotContaining("Group By")
                    .returns("""
                            id\taid
                            1\t10
                            3\t11
                            """);
            assertQuery("SELECT o.id, l.aid FROM (SELECT id, x + 0 x FROM o) o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.x = o.x AND a.id > o.x) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanNotContaining("Group By")
                    .returns("""
                            id\taid
                            1\t10
                            3\t11
                            """);
        });
    }

    // The body under SELECT * FROM (...).
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonWrapped() throws Exception {
        assertKeyAndComparison(
                false,
                "SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT * FROM (SELECT a.id aid FROM a WHERE a.x = o.x AND a.id > o.x)) l ORDER BY 1, 2",
                """
                        id\taid
                        1\t10
                        3\t11
                        """
        );
    }

    // Other conditions that read the outer column next to the key, with the rows of the body run
    // once per outer row: OR keeps a row 12 of the NULL o.x by a.id = 12; NOT (a.id > o.x) keeps
    // a row 0 only, as a comparison with NULL is false and so is its negation here; IN keeps a
    // row 11; CASE keeps rows 10 and 11; coalesce(o.x, 3) = 3 keeps the rows of the NULL o.x and of
    // o.x = 3; an equality between o.x and an expression keeps a row 10, whose id - 9 is 1.
    @Test
    public void testLateralWhereKeyAndCorrelatedConditionForms() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            final String[][] cases = {
                    {"(a.id > o.x OR a.id = 12)", "1\t10\n2\t12\n3\t11\n"},
                    {"NOT (a.id > o.x)", "1\t0\n"},
                    {"a.id IN (o.x, 11)", "3\t11\n"},
                    {"CASE WHEN a.id > o.x THEN true ELSE false END", "1\t10\n3\t11\n"},
                    {"coalesce(o.x, 3) = 3", "2\t12\n3\t11\n"},
                    {"o.x = a.id - 9", "1\t10\n"},
                    {"a.id - 9 = o.x", "1\t10\n"},
            };
            for (String[] c : cases) {
                assertQuery("SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.x = o.x AND " + c[0] + ") l ORDER BY 1, 2")
                        .noLeakCheck()
                        .returns("id\taid\n" + c[1]);
            }
        });
    }

    @Test
    public void testLateralWhereKeyAndOuterOnlyCondition() throws Exception {
        assertKeyAndComparison(
                false,
                "SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.x = o.x AND o.x > 1) l ORDER BY 1, 2",
                """
                        id\taid
                        3\t11
                        """
        );
    }

    // The WHERE key reads bk, an alias of b.k that only the layer above the joins has, and the joins
    // compare c.id with o.x. Per outer row, a rows 10, 11 and 12 match c rows 100, 101 and 103 by
    // c.sid = a.id + 11, and a row 10 matches b rows 21 and 23, a row 11 b row 24 by a.x = b.k.
    // c.id > o.x + 98 keeps c rows 100, 101 and 103 for o.x = 1, and c row 103 only for o.x = 3,
    // whose a row 12 matches no b row, so bk = o.x keeps the two rows of a row 10 for o row 1.
    // DuckDB, with IS NOT DISTINCT FROM for =, returns 1 10 1 twice.
    @Test
    public void testLateralWhereKeyOnUpperLayerAliasAndCorrelatedCondition() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.aid, l.bk FROM o JOIN LATERAL (SELECT * FROM (SELECT a.id aid, b.k bk FROM a JOIN c ON c.sid = a.id + 11 JOIN b ON a.x = b.k WHERE c.id > o.x + 98) WHERE bk = o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .returns("""
                            id\taid\tbk
                            1\t10\t1
                            1\t10\t1
                            """);
        });
    }

    // The query of testLateralWhereKeyOnUpperLayerAliasAndCorrelatedCondition() with the alias sid,
    // which c has as a column too. The WHERE key still reads b.k through the alias, so DuckDB, with
    // IS NOT DISTINCT FROM for =, returns 1 10 1 twice, and LEFT JOIN LATERAL adds o rows 2 and 3
    // with no row of the body.
    @Test
    public void testLateralWhereKeyOnUpperLayerAliasNamedLikeJoinColumn() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            final String body = "(SELECT * FROM (SELECT a.id aid, b.k sid FROM a JOIN c ON c.sid = a.id + 11 JOIN b ON a.x = b.k WHERE c.id > o.x + 98) WHERE sid = o.x) l ORDER BY 1, 2, 3";
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL " + body)
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            1\t10\t1
                            1\t10\t1
                            """);
            assertQuery("SELECT o.id, l.aid, l.sid FROM o LEFT JOIN LATERAL " + body)
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            1\t10\t1
                            1\t10\t1
                            2\tnull\tnull
                            3\tnull\tnull
                            """);
        });
    }

    // c's ON clause reads o.x before a RIGHT join, and the WHERE key of the layer above reads sid, an
    // alias of b.k, the slave of that join, which c has as a column too. Per outer row, o.x > 1 leaves
    // the master side of the RIGHT join empty for o.x = 1 and NULL, so a.id IS NOT NULL drops every
    // row there, and for o.x = 3 a row 11 matches b row 24, whose k is 3. DuckDB returns 3 11 3, as a
    // LATERAL query with IS NOT DISTINCT FROM for QuestDB's = and as the body run once per outer row.
    @Test
    public void testLateralWhereKeyOnUpperLayerAliasNamedLikeJoinColumnBeforeRightJoin() throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery("SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (SELECT * FROM (SELECT a.id aid, b.k sid FROM a JOIN c ON c.sid = a.id + 11 AND o.x > 1 RIGHT JOIN b ON a.x = b.k WHERE a.id IS NOT NULL) WHERE sid = o.x) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            3\t11\t3
                            """);
        });
    }

    // Both equalities read o.x: a.x = o.x and a.id = o.x. Per outer row, only a row 1 has both
    // a.x = 1 and a.id = 1; without either equality, a row 3 would match too.
    @Test
    public void testLateralWhereTwoKeysOnOneOuterColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, NULL), (3, 3)");
            execute("INSERT INTO a VALUES (1, 1), (3, 1)");
            assertQuery("SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.x = o.x AND a.id = o.x) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .returns("""
                            id\taid
                            1\t1
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.val, sub.qty
                    FROM t_outer o
                    JOIN LATERAL (
                        SELECT a.val, b.qty
                        FROM (SELECT val FROM t_a WHERE t_a.oid = o.id) a
                        CROSS JOIN (SELECT qty FROM t_b WHERE t_b.oid = o.id) b
                    ) sub ON true
                    ORDER BY o.id, sub.val
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tval\tqty
                            1\t10\t100
                            1\t20\t100
                            2\t30\t200
                            """);
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

            assertQuery("""
                    SELECT o.id, sub.val, sub.info
                    FROM t_outer o
                    JOIN LATERAL (
                        SELECT l.val, r.info
                        FROM t_left l
                        LEFT JOIN t_right r ON l.id = r.ref_id AND r.id = o.id
                        WHERE l.id = o.id
                    ) sub ON true
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tval\tinfo
                            1\t10\tinfo_a
                            2\t20\tinfo_b
                            """);
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

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tbucket
                            1\t10.0\tsmall
                            1\t25.0\tsmall
                            1\t40.0\tlarge
                            2\t15.0\tsmall
                            2\t50.0\tlarge
                            3\t29.0\tsmall
                            3\t30.0\tlarge
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.val
                    FROM t_outer o
                    JOIN LATERAL (
                        (SELECT val FROM t_data WHERE oid = o.id AND src = 'A' ORDER BY val LIMIT 1)
                        UNION ALL
                        (SELECT val FROM t_data WHERE oid = o.id AND src = 'B' ORDER BY val LIMIT 1)
                    ) sub ON true
                    ORDER BY o.id, sub.val
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tval
                            1\t10
                            1\t40
                            2\t60
                            2\t70
                            """);
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

            assertQuery("""
                    SELECT t1.x, sub.val FROM t1
                    CROSS JOIN LATERAL (
                        SELECT a AS val FROM t2 WHERE t2.x = t1.x
                        UNION ALL
                        SELECT total FROM (
                            SELECT sum(b) AS total FROM t3 WHERE t3.x = t1.x GROUP BY cat
                        ) agg WHERE agg.total > 100
                    ) sub
                    ORDER BY t1.x, sub.val
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tval
                            1\t10
                            1\t200
                            2\t20
                            2\t700
                            """);
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

            assertQuery("""
                    SELECT t1.x, sub.val FROM t1
                    CROSS JOIN LATERAL (
                        SELECT sum(a) AS val FROM t2 WHERE t2.x = t1.x
                        UNION ALL
                        SELECT s FROM (
                            SELECT sum(b) AS s FROM t3 WHERE t3.x = t1.x
                        ) nested_sub
                    ) sub
                    ORDER BY t1.x, sub.val
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tval
                            1\t30
                            1\t100
                            2\t30
                            2\t500
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tval
                            1\t10
                            1\t20
                            2\t30
                            2\t40
                            """);
        });
    }

    @Test
    public void testLeftLateralApproxCountDistinctZeroOnNoMatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE fills (order_id INT, venue_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO fills VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (1, 10, '2024-01-01T00:30:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.id, sub.venues
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT approx_count_distinct(venue_id) AS venues
                        FROM fills
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tvenues
                            1\t2
                            2\t1
                            3\t0
                            """);
        });
    }

    @Test
    public void testLeftLateralCoalesceSumBodyNotCompensated() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 30, '2024-01-01T01:10:00.000000Z')
                    """);

            // only zero-on-empty aggregates are compensated; a null-absorbing wrapper over
            // sum() is not, so the unmatched row keeps NULL rather than coalesce(NULL, 0) = 0
            assertQuery("""
                    SELECT o.id, sub.s
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT coalesce(sum(qty), 0) AS s FROM trades WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ts
                            1\t30
                            2\t30
                            3\tnull
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 40, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.id, sub.arithmetic
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tarithmetic
                            1\t4
                            2\t3
                            3\t2
                            """);

            assertQuery("""
                    SELECT o.id, sub.c, sub.d
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS c, count(*) + 2 AS d
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc\td
                            1\t2\t4
                            2\t1\t3
                            3\t0\t2
                            """);

            assertQuery("""
                    SELECT o.id, sub.arithmetic * 10 AS scaled
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY sub.arithmetic, o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tscaled
                            3\t20
                            2\t30
                            1\t40
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticAggregateOverInnerLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t1 (k INT)");
            execute("CREATE TABLE trades (order_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("INSERT INTO t1 VALUES (1)");
            execute("""
                    INSERT INTO trades VALUES
                    (1, '2024-01-01T00:10:00.000000Z'),
                    (1, '2024-01-01T00:20:00.000000Z'),
                    (2, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.id, sub.v
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS v
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count(*) AS c FROM trades WHERE order_id = o.id
                        ) s
                        WHERE t1.k = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t3
                            2\t2
                            3\t2
                            """);
        });
    }

    // bind variables are captured by compensation templates, so the projection
    // remains compensated on unmatched rows
    @Test
    public void testLeftLateralCountArithmeticBindVariable() throws Exception {
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

            bindVariableService.clear();
            bindVariableService.setLong(0, 2L);
            assertQuery("""
                    SELECT o.id, sub.v
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + $1 AS v
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t4
                            2\t3
                            3\t2
                            """);

            bindVariableService.clear();
            bindVariableService.setLong(0, 0L);
            assertQuery("""
                    SELECT o.id, sub.cnt
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    WHERE sub.cnt = $1
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcnt
                            3\t0
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticConstantColumnCompensated() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, venue SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 'LSE', '2024-01-01T00:10:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.id, sub.c, sub.z, sub.oid
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS c, 5 AS z, o.id AS oid FROM trades WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc\tz\toid
                            1\t1\t5\t1
                            2\t0\t5\t2
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticDistinctConsumer() throws Exception {
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

            assertQuery("""
                    SELECT DISTINCT o.id, sub.arithmetic
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tarithmetic
                            1\t4
                            2\t3
                            3\t2
                            """);

            assertQuery("""
                    SELECT DISTINCT o.id, sub.cnt
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcnt
                            1\t2
                            2\t1
                            3\t0
                            """);

            assertQuery("""
                    SELECT DISTINCT o.id, sub.cnt
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT c.* FROM (
                            SELECT count(*) AS cnt
                            FROM trades
                            WHERE order_id = o.id
                        ) c
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcnt
                            1\t2
                            2\t1
                            3\t0
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticFilteredByParent() throws Exception {
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

            assertQuery("""
                    SELECT o.id, sub.arithmetic
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    WHERE sub.arithmetic = 2
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tarithmetic
                            3\t2
                            """);

            assertQuery("""
                    SELECT o.id, sub.cnt
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    WHERE sub.cnt = 0
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcnt
                            3\t0
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticGroupByOrderByQualified() throws Exception {
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

            assertQuery("""
                    SELECT sub.arithmetic, count(*) AS n
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    GROUP BY sub.arithmetic
                    ORDER BY sub.arithmetic
                    """)
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            arithmetic\tn
                            2\t1
                            3\t1
                            4\t1
                            """);

            assertQuery("""
                    SELECT sub.arithmetic AS va, count(*) AS n
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    GROUP BY sub.arithmetic
                    ORDER BY sub.arithmetic DESC
                    """)
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            va\tn
                            4\t1
                            3\t1
                            2\t1
                            """);

            assertQuery("""
                    SELECT sub.cnt, count(*) AS n
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    GROUP BY sub.cnt
                    ORDER BY sub.cnt
                    """)
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            cnt\tn
                            0\t1
                            1\t1
                            2\t1
                            """);

            assertQuery("""
                    SELECT o.id, sub.arithmetic
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY sub.arithmetic, o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tarithmetic
                            3\t2
                            2\t3
                            1\t4
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticImplicitKeyNotCompensated() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, venue SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 'LSE', '2024-01-01T00:10:00.000000Z')
                    """);

            // the body groups by venue, so an unmatched outer row yields no inner row at all
            assertQuery("""
                    SELECT o.id, sub.k, sub.c
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT venue AS k, count(*) + 2 AS c FROM trades WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tk\tc
                            1\tLSE\t3
                            2\t\tnull
                            """);

            assertQuery("""
                    SELECT o.id, sub.k, sub.c
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT venue AS k, approx_count_distinct(order_id) AS c FROM trades WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tk\tc
                            1\tLSE\t1
                            2\t\tnull
                            """);

            assertQuery("""
                    SELECT o.id, sub.c
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS c FROM trades WHERE order_id = o.id SAMPLE BY 1h
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc
                            1\t3
                            2\tnull
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticInCte() throws Exception {
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

            assertQuery("""
                    WITH w AS (
                        SELECT o.id AS id, sub.arithmetic AS v
                        FROM orders o
                        LEFT JOIN LATERAL (
                            SELECT count(*) + 2 AS arithmetic
                            FROM trades
                            WHERE order_id = o.id
                        ) sub
                    )
                    SELECT * FROM w ORDER BY id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t4
                            2\t3
                            3\t2
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticMixedAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 40, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.id, sub.mixed
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + sum(qty) AS mixed
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tmixed
                            1\t32
                            2\t41
                            3\tnull
                            """);

            assertQuery("""
                    SELECT o.id, sub.val
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT CASE WHEN count() = 0 THEN 2 ELSE sum(qty) END AS val
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\t30
                            2\t40
                            3\t2
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticMultipleJoins() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE fills (order_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
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
            execute("INSERT INTO fills VALUES (1, '2024-01-01T00:10:00.000000Z')");

            assertQuery("""
                    SELECT o.id, s1.v, s2.v
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS v FROM trades WHERE order_id = o.id
                    ) s1
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 5 AS v FROM fills WHERE order_id = o.id
                    ) s2
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv\tv1
                            1\t4\t6
                            2\t3\t5
                            3\t2\t5
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticNegativeGuards() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 40, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.id, sub.v
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT sum(qty) + 1 AS v
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .withPlanNotContaining("coalesce")
                    .returns("""
                            id\tv
                            1\t31
                            2\t41
                            3\tnull
                            """);

            assertQuery("""
                    SELECT o.id, sub.v
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS v
                        FROM trades
                        WHERE order_id = o.id
                        LIMIT 0
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\tnull
                            2\tnull
                            3\tnull
                            """);

            assertQuery("""
                    SELECT o.id, sub.v
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS v
                        FROM trades
                        WHERE order_id = o.id
                        GROUP BY order_id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t4
                            2\t3
                            3\tnull
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticNestedBodyDoesNotExplode() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, venue SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 'LSE', '2024-01-01T00:10:00.000000Z')
                    """);

            StringBuilder body = new StringBuilder("SELECT count(*) AS v0 FROM trades WHERE order_id = o.id");
            for (int i = 1; i <= 24; i++) {
                body.insert(0, "SELECT v" + (i - 1) + " + v" + (i - 1) + " AS v" + i + " FROM (").append(')');
            }
            // the template budget degrades to uncompensated NULL instead of expanding 2^24 nodes
            assertQuery("SELECT o.id, sub.v24 FROM orders o LEFT JOIN LATERAL (" + body + ") sub ORDER BY o.id")
                    .noLeakCheck()
                    .returns("""
                            id\tv24
                            1\t16777216
                            2\tnull
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticOnRejectsRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t1.k, l.v
                    FROM t1
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS v
                        FROM t2
                        WHERE t2.k = t1.k
                    ) l ON l.v > 2
                    ORDER BY t1.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\tv
                            1\t3
                            2\tnull
                            """);
        });
    }

    // Known limitation: compensation does not reach a column referenced only in
    // ORDER BY, so the unmatched row sorts as NULL (first) instead of as 10.
    @Test
    public void testLeftLateralCountArithmeticOrderByOnlyNotCompensated() throws Exception {
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

            assertQuery("""
                    SELECT o.id
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT 10 - count(*) AS inv
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY sub.inv, o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id
                            3
                            1
                            2
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticOuterRef() throws Exception {
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

            assertQuery("""
                    SELECT o.id, sub.val
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) * 10 + o.id AS val
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\t21
                            2\t12
                            3\t3
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticRenamed() throws Exception {
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

            assertQuery("""
                    SELECT o.id, sub.arithmetic AS foo
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tfoo
                            1\t4
                            2\t3
                            3\t2
                            """);

            assertQuery("""
                    SELECT sub.arithmetic AS v, count(*) AS n
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    GROUP BY sub.arithmetic
                    ORDER BY v
                    """)
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            v\tn
                            2\t1
                            3\t1
                            4\t1
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticRepeatedLeaf() throws Exception {
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

            assertQuery("""
                    SELECT o.id, sub.v
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + count(*) AS v
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .withPlanContaining("values: [count(*)]")
                    .returns("""
                            id\tv
                            1\t4
                            2\t2
                            3\t0
                            """);
        });
    }

    // A CROSS JOIN over a provably one-row relation above the aggregate keeps the
    // count row; the carrier column threads through the join-layer projection.
    @Test
    public void testLeftLateralCountArithmeticSingleRowJoinBody() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT)");
            execute("CREATE TABLE trades (order_id INT)");
            execute("INSERT INTO orders VALUES (1), (3)");
            execute("INSERT INTO trades VALUES (1)");

            assertQuery("""
                    SELECT o.id, sub.v
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT c * 2 AS v
                        FROM (SELECT count(*) AS c FROM trades WHERE order_id = o.id) a
                        CROSS JOIN (SELECT 1 AS one) b
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t2
                            3\t0
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticSumWithOuterRef() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 40, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.id, sub.v
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + sum(qty + o.id) AS v
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t34
                            2\t43
                            3\tnull
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticUnaliased() throws Exception {
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

            assertQuery("""
                    SELECT o.id, arithmetic
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    )
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tarithmetic
                            1\t4
                            2\t3
                            3\t2
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticUnionConsumer() throws Exception {
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

            assertQuery("""
                    SELECT o.id, sub.arithmetic
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    UNION ALL
                    SELECT o.id + 10, sub.arithmetic
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tarithmetic
                            1\t4
                            2\t3
                            3\t2
                            11\t4
                            12\t3
                            13\t2
                            """);

            assertQuery("""
                    SELECT o.id, sub.cnt
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    UNION
                    SELECT 99, 0 FROM long_sequence(1)
                    ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcnt
                            1\t2
                            2\t1
                            3\t0
                            99\t0
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticWhereAliasShadow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, x INT)");
            execute("INSERT INTO t1 VALUES (1, 10), (2, 1), (3, 1)");
            execute("CREATE TABLE t2 (id INT, y INT)");
            execute("INSERT INTO t2 VALUES (1, 1), (2, 10), (3, 10)");
            execute("CREATE TABLE t3 (id INT)");
            execute("INSERT INTO t3 VALUES (1), (2)");

            assertQuery("""
                    SELECT t2.y AS x, sub.cnt
                    FROM t1
                    JOIN t2 ON t2.id = t1.id
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM t3
                        WHERE t3.id = t1.id
                    ) sub
                    WHERE sub.cnt = 0 OR x > 5
                    ORDER BY t1.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            x\tcnt
                            1\t1
                            10\t0
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticWhereMemoizesCarrier() throws Exception {
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
        allowFunctionMemoization();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP)");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '1970-01-01T00:00:00.000000Z'),
                    (2, '1970-01-01T00:00:00.000001Z'),
                    (3, '1970-01-01T00:00:00.000002Z')
                    """);
            execute("CREATE TABLE trades (order_id INT)");

            final String sql = """
                    SELECT o.id, sub.v
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + test_timestamp_counter(o.ts)::long AS v
                        FROM trades
                        WHERE order_id = o.id
                        LIMIT :lim
                    ) sub
                    WHERE sub.v >= 0 AND sub.v >= 0
                    ORDER BY o.id
                    """;

            bindVariableService.setLong("lim", 1);
            assertQuery(sql).noLeakCheck().assertsPlanContaining(
                    "Filter filter: (v>=0 and v>=0)",
                    "memoize(case([__lateral_limit(:lim"
            );
            try (RecordCursorFactory factory = select(sql)) {
                TestTimestampCounterFactory.COUNTER.set(0);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursor("id\tv\n1\t0\n2\t1\n3\t2\n", cursor, factory.getMetadata(), true);
                }
                Assert.assertEquals(3, TestTimestampCounterFactory.COUNTER.get());

                bindVariableService.setLong("lim", 0);
                TestTimestampCounterFactory.COUNTER.set(0);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursor("id\tv\n", cursor, factory.getMetadata(), true);
                }
                Assert.assertEquals(0, TestTimestampCounterFactory.COUNTER.get());

                bindVariableService.setLong("lim", 1);
                TestTimestampCounterFactory.COUNTER.set(0);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursor("id\tv\n1\t0\n2\t1\n3\t2\n", cursor, factory.getMetadata(), true);
                }
                Assert.assertEquals(3, TestTimestampCounterFactory.COUNTER.get());
            }
        });
    }

    @Test
    public void testLeftLateralCountArithmeticWhereMixedTerms() throws Exception {
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

            assertQuery("""
                    SELECT o.id, sub.arithmetic
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    WHERE sub.arithmetic = 2 OR o.id = 1
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tarithmetic
                            1\t4
                            3\t2
                            """);

            assertQuery("""
                    SELECT o.id, sub.arithmetic
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    WHERE sub.arithmetic = 2 AND o.id > 1
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tarithmetic
                            3\t2
                            """);

            assertQuery("""
                    SELECT o.id, sub.arithmetic
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    WHERE sub.arithmetic - o.id = -1
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tarithmetic
                            3\t2
                            """);

            assertQuery("""
                    SELECT o.id, sub.arithmetic
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    WHERE sub.arithmetic - o.id = 1
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tarithmetic
                            2\t3
                            """);

            assertQuery("""
                    SELECT o.id, sub.arithmetic
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    WHERE sub.arithmetic = 2 OR o.ts > '2024-06-01'
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tarithmetic
                            3\t2
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticWhereOnlyRef() throws Exception {
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

            assertQuery("""
                    SELECT o.id
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    WHERE sub.cnt = 0
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id
                            3
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticWhereUnqualified() throws Exception {
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

            assertQuery("""
                    SELECT o.id, sub.arithmetic
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    WHERE arithmetic = 2
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tarithmetic
                            3\t2
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticWildcard() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("INSERT INTO trades VALUES (1, '2024-01-01T00:10:00.000000Z')");

            assertQuery("""
                    SELECT o.id, sub.*
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tarithmetic
                            1\t3
                            2\t2
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticWildcardRenamed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, qty LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 40, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.id, sub.*
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT c.other AS v, c.*
                        FROM (
                            SELECT min(qty) AS other, count(*) + 2 AS v
                            FROM trades
                            WHERE order_id = o.id
                        ) c
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv\tother\tv1
                            1\t10\t10\t4
                            2\t40\t40\t3
                            3\tnull\tnull\t2
                            """);

            assertQuery("""
                    SELECT o.id, sub.v1
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT c.other AS v, c.*
                        FROM (
                            SELECT min(qty) AS other, count(*) + 2 AS v
                            FROM trades
                            WHERE order_id = o.id
                        ) c
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv1
                            1\t4
                            2\t3
                            3\t2
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticWrapped() throws Exception {
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

            assertQuery("""
                    SELECT o.id, sub.arithmetic
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT c + 2 AS arithmetic FROM (
                            SELECT count(*) AS c
                            FROM trades
                            WHERE order_id = o.id
                        )
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tarithmetic
                            1\t4
                            2\t3
                            3\t2
                            """);
        });
    }

    @Test
    public void testLeftLateralCountArithmeticWrappedSubqueryWhere() throws Exception {
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

            assertQuery("""
                    SELECT * FROM (
                        SELECT o.id, sub.cnt
                        FROM orders o
                        LEFT JOIN LATERAL (
                            SELECT count(*) AS cnt
                            FROM trades
                            WHERE order_id = o.id
                        ) sub
                    ) WHERE cnt = 0
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tcnt
                            3\t0
                            """);

            assertQuery("""
                    SELECT * FROM (
                        SELECT o.id, sub.arithmetic
                        FROM orders o
                        LEFT JOIN LATERAL (
                            SELECT count(*) + 2 AS arithmetic
                            FROM trades
                            WHERE order_id = o.id
                        ) sub
                    ) WHERE arithmetic = 2
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tarithmetic
                            3\t2
                            """);
        });
    }

    @Test
    public void testLeftLateralCountCarrierAliasCollision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT)");
            execute("CREATE TABLE trades (order_id INT)");
            execute("INSERT INTO orders VALUES (1), (2)");
            execute("INSERT INTO trades VALUES (1)");

            assertQuery("""
                    SELECT o.id, sub.arithmetic, sub."__qdb_count_carrier__0"
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic, count(*) AS "__qdb_count_carrier__0"
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tarithmetic\t__qdb_count_carrier__0
                            1\t3\t1
                            2\t2\t0
                            """);

            assertQuery("""
                    SELECT o.id, sub.arithmetic, sub."__qdb_count_carrier__0"
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT c.arithmetic, c.cnt AS "__qdb_count_carrier__0"
                        FROM (
                            SELECT count(*) + 2 AS arithmetic, count(*) AS cnt
                            FROM trades
                            WHERE order_id = o.id
                        ) c
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tarithmetic\t__qdb_count_carrier__0
                            1\t3\t1
                            2\t2\t0
                            """);
        });
    }

    @Test
    public void testLeftLateralCountCase() throws Exception {
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

            assertQuery("""
                    SELECT o.id, sub.val
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT CASE WHEN count() = 0 THEN 7 ELSE count() END AS val
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\t2
                            2\t1
                            3\t7
                            """);
        });
    }

    @Test
    public void testLeftLateralCountCoalesceWrapped() throws Exception {
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

            assertQuery("""
                    SELECT o.id, sub.v
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT coalesce(count(*), 5) AS v
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t2
                            2\t1
                            3\t0
                            """);
        });
    }

    @Test
    public void testLeftLateralCountDistinctAndApproxCountDistinctOnRejectsRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1), (2)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t1.k, l.v
                    FROM t1
                    LEFT JOIN LATERAL (
                        SELECT count_distinct(t2.k) AS v
                        FROM t2
                        WHERE t2.k = t1.k
                    ) l ON true
                    ORDER BY t1.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\tv
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT t1.k, l.exact_v, l.approx_v
                    FROM t1
                    LEFT JOIN LATERAL (
                        SELECT count_distinct(t2.k) AS exact_v, approx_count_distinct(t2.k) AS approx_v
                        FROM t2
                        WHERE t2.k = t1.k
                    ) l ON l.exact_v > 0 AND l.approx_v > 0
                    ORDER BY t1.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\texact_v\tapprox_v
                            1\t1\t1
                            2\tnull\tnull
                            """);
        });
    }

    @Test
    public void testLeftLateralCountDistinctArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE fills (order_id INT, venue_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO fills VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (1, 10, '2024-01-01T00:30:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.id, sub.venues
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count_distinct(venue_id) + 1 AS venues
                        FROM fills
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .withPlanContaining("values: [count_distinct(venue_id)]")
                    .returns("""
                            id\tvenues
                            1\t3
                            2\t2
                            3\t1
                            """);
        });
    }

    @Test
    public void testLeftLateralCountDistinctZeroOnNoMatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE fills (order_id INT, venue_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO fills VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (1, 10, '2024-01-01T00:30:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.id, sub.venues
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count_distinct(venue_id) AS venues
                        FROM fills
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tvenues
                            1\t2
                            2\t1
                            3\t0
                            """);

            assertQuery("""
                    SELECT o.id, sub.venues, sub.total
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count_distinct(venue_id) AS venues, sum(venue_id) AS total
                        FROM fills
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tvenues\ttotal
                            1\t2\t40
                            2\t1\t20
                            3\t0\tnull
                            """);
        });
    }

    @Test
    public void testLeftLateralCountMarkerAliasCollision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.*
                        FROM (
                            SELECT k, 100 + k AS "__qdb_count_marker__1"
                            FROM t1
                        ) t1
                        LEFT JOIN LATERAL (
                            SELECT c.*
                            FROM (
                                SELECT count(*) + t0.a AS val
                                FROM t2
                                WHERE t2.x = t0.a
                            ) c
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a	val
                            1	3
                            2	2
                            """);
        });
    }

    @Test
    public void testLeftLateralCountMarkerAliasCollisionFromWildcardSibling() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE outer_tab (id INT)");
            execute("INSERT INTO outer_tab VALUES (1), (2)");
            execute("CREATE TABLE inner_tab (id INT)");
            execute("INSERT INTO inner_tab VALUES (1)");

            String lateralJoin = """
                    FROM outer_tab o
                    LEFT JOIN LATERAL (
                        SELECT *
                        FROM (
                            SELECT count(*) AS cnt
                            FROM inner_tab i
                            WHERE i.id = o.id
                        ) counted
                        CROSS JOIN (
                            SELECT 701 AS "__qdb_count_marker__0"
                            FROM long_sequence(1)
                        ) sibling
                    ) l ON l.cnt >= 0
                    """;

            assertQuery("SELECT o.id, l.cnt\n" + lateralJoin + "ORDER BY o.id")
                    .noLeakCheck()
                    .returns("""
                            id\tcnt
                            1\t1
                            2\t0
                            """);

            assertQuery("SELECT o.id, l.\"__qdb_count_marker__0\"\n"
                    + lateralJoin
                    + "WHERE o.id = 1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\t__qdb_count_marker__0
                            1\t701
                            """);
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

            assertQuery("""
                    SELECT timestamp, t.symbol, price, c FROM fx_trades t
                    LEFT JOIN LATERAL (
                        SELECT count() c FROM fx_trades
                        WHERE symbol = t.symbol AND price > (t.price * 1.01)
                    ) u
                    ORDER BY timestamp
                    """)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .returns("""
                            timestamp\tsymbol\tprice\tc
                            2024-01-01T00:00:00.000000Z\tEUR/USD\t1.1\t1
                            2024-01-01T01:00:00.000000Z\tEUR/USD\t1.2\t0
                            2024-01-01T02:00:00.000000Z\tGBP/USD\t1.3\t0
                            """);

            assertQuery("""
                    SELECT timestamp, t.symbol, price, c FROM fx_trades t
                    LEFT JOIN LATERAL (
                        SELECT count() c FROM fx_trades
                        WHERE symbol = t.symbol AND price > (t.price * 1.01)
                    )
                    ORDER BY timestamp
                    """)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .returns("""
                            timestamp\tsymbol\tprice\tc
                            2024-01-01T00:00:00.000000Z\tEUR/USD\t1.1\t1
                            2024-01-01T01:00:00.000000Z\tEUR/USD\t1.2\t0
                            2024-01-01T02:00:00.000000Z\tGBP/USD\t1.3\t0
                            """);

            assertQuery("""
                    SELECT timestamp, t.symbol, price, u.symbol, c FROM fx_trades t
                    LEFT JOIN LATERAL (
                        SELECT symbol, count() c FROM fx_trades
                        WHERE symbol = t.symbol AND price > (t.price * 1.01)
                    ) u
                    ORDER BY timestamp
                    """)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .returns("""
                            timestamp\tsymbol\tprice\tsymbol1\tc
                            2024-01-01T00:00:00.000000Z\tEUR/USD\t1.1\tEUR/USD\t1
                            2024-01-01T01:00:00.000000Z\tEUR/USD\t1.2\t\tnull
                            2024-01-01T02:00:00.000000Z\tGBP/USD\t1.3\t\tnull
                            """);
        });
    }

    @Test
    public void testLeftLateralCountQuotedDottedAlias() throws Exception {
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

            assertQuery("""
                    SELECT o.id, sub."c.dot"
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS "c.dot"
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc.dot
                            1\t2
                            2\t1
                            3\t0
                            """);
        });
    }

    // Every column of a scalar aggregate body takes its empty-group value for an
    // unmatched outer row, not just the bare count (#7460).
    @Test
    public void testLeftLateralCountSiblingColumnsCompensated() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT)");
            execute("CREATE TABLE trades (order_id INT, qty LONG)");
            execute("INSERT INTO orders VALUES (1), (3)");
            execute("INSERT INTO trades VALUES (1, 10)");

            assertQuery("""
                    SELECT o.id, sub.cnt, sub.tag
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt, 'T' AS tag
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcnt\ttag
                            1\t1\tT
                            3\t0\tT
                            """);

            assertQuery("""
                    SELECT o.id, sub.tag, sub.cnt
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT 'T' AS tag, count(*) AS cnt
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ttag\tcnt
                            1\tT\t1
                            3\tT\t0
                            """);

            assertQuery("""
                    SELECT o.id, sub.cnt, sub.arith
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt, count(*) + 2 AS arith
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcnt\tarith
                            1\t1\t3
                            3\t0\t2
                            """);

            assertQuery("""
                    SELECT o.id, sub.cnt, sub.mx
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt, max(qty) AS mx
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcnt\tmx
                            1\t1\t10
                            3\t0\tnull
                            """);

            assertQuery("""
                    SELECT o.id, sub.mx, sub.tag
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT max(qty) AS mx, 'T' AS tag
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tmx\ttag
                            1\t10\tT
                            3\tnull\tT
                            """);
        });
    }

    @Test
    public void testLeftLateralCountWildcardNonTrivialOn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE outer_tab (id INT)");
            execute("INSERT INTO outer_tab VALUES (1), (2)");
            execute("CREATE TABLE inner_tab (id INT)");
            execute("INSERT INTO inner_tab VALUES (1)");

            assertQuery("""
                    SELECT o.id, x.c
                    FROM outer_tab o
                    LEFT JOIN LATERAL (
                        SELECT *
                        FROM (
                            SELECT count(*) AS c
                            FROM inner_tab i
                            WHERE i.id = o.id
                        ) counted
                    ) x ON 1 = 1
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT o.id, x.c
                    FROM outer_tab o
                    LEFT JOIN LATERAL (
                        SELECT *
                        FROM (
                            SELECT *
                            FROM (
                                SELECT count(*) AS c
                                FROM inner_tab i
                                WHERE i.id = o.id
                            ) counted
                        ) wrapped
                    ) x ON x.c >= 0
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT o.id, x.d
                    FROM outer_tab o
                    LEFT JOIN LATERAL (
                        SELECT *
                        FROM (
                            SELECT c AS d
                            FROM (
                                SELECT *
                                FROM (
                                    SELECT count(*) AS c
                                    FROM inner_tab i
                                    WHERE i.id = o.id
                                ) counted
                            ) passed
                        ) renamed
                    ) x ON 1 = 1
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\td
                            1\t1
                            2\t0
                            """);
        });
    }

    @Test
    public void testLeftLateralCountWildcardRepeatedStarLayers() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE outer_tab (id INT)");
            execute("INSERT INTO outer_tab VALUES (1), (2)");
            execute("CREATE TABLE inner_tab (id INT)");
            execute("INSERT INTO inner_tab VALUES (1)");

            assertQuery("""
                    SELECT o.id, x.c, x.c_plus_one
                    FROM outer_tab o
                    LEFT JOIN LATERAL (
                        SELECT wrapped2.*, wrapped2.*
                        FROM (
                            SELECT wrapped1.*, wrapped1.*
                            FROM (
                                SELECT counted.*, counted.*
                                FROM (
                                    SELECT count(*) AS c, count(*) + 1 AS c_plus_one
                                    FROM inner_tab i
                                    WHERE i.id = o.id
                                ) counted
                            ) wrapped1
                        ) wrapped2
                    ) x ON true
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc\tc_plus_one
                            1\t1\t2
                            2\t0\t1
                            """);

            assertQuery("""
                    SELECT o.id, x.c, x.c_plus_one
                    FROM outer_tab o
                    LEFT JOIN LATERAL (
                        SELECT *, *
                        FROM (
                            SELECT *, *
                            FROM (
                                SELECT *, *
                                FROM (
                                    SELECT count(*) AS c, count(*) + 1 AS c_plus_one
                                    FROM inner_tab i
                                    WHERE i.id = o.id
                                ) counted
                            ) wrapped1
                        ) wrapped2
                    ) x ON true
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc\tc_plus_one
                            1\t1\t2
                            2\t0\t1
                            """);
        });
    }

    @Test
    public void testLeftLateralUnionCountBodyNotCompensated() throws Exception {
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

            // a correlated empty UNION-ALL branch defeats the zero-rejection proof, so the
            // count body is not admitted for compensation and the unmatched row keeps NULL
            assertQuery("""
                    SELECT o.id, sub.c
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS c FROM trades WHERE order_id = o.id
                        UNION ALL
                        SELECT 9 AS c FROM trades WHERE order_id = o.id AND 1 = 0
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc
                            1\t2
                            2\t1
                            3\tnull
                            """);
        });
    }

    // Window functions have no meaningful zero-on-empty value; template extraction
    // must reject them and keep the uncompensated NULL on unmatched rows.
    @Test
    public void testLeftLateralWindowFunctionBodyNotCompensated() throws Exception {
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

            assertQuery("""
                    SELECT o.id, sub.wcnt
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT wcnt FROM (
                            SELECT count(*) OVER () AS wcnt, cnt FROM (
                                SELECT count(*) AS cnt
                                FROM trades
                                WHERE order_id = o.id
                            )
                        )
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\twcnt
                            1\t1
                            2\t1
                            3\tnull
                            """);

            assertQuery("""
                    SELECT o.id, sub.wv
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT wcnt + cnt AS wv FROM (
                            SELECT row_number() OVER () AS wcnt, cnt FROM (
                                SELECT count(*) AS cnt
                                FROM trades
                                WHERE order_id = o.id
                            )
                        )
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\twv
                            1\t3
                            2\t2
                            3\tnull
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountAliasLessCollision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (cnt INT)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        )
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\tnull
                            2\tnull
                            """);

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.cnt
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        )
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\tnull
                            2\tnull
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountAliasLessSurfaces() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        )
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.c2
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt AS c2
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        )
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tc2
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        )
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT *
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        )
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);
        });
    }

    // C1 companion - the count body as the SECOND join model already compensates
    // to 0. Pinned so the keeps-the-row fix cannot be taken by suppressing this
    // path instead, and so a plain revert of the wrapper handling is caught here.
    @Test
    public void testNestedLateralLeftCountAsSecondJoinModel() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT counted.cnt
                        FROM (SELECT 1 AS z) d
                        CROSS JOIN (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t2
                            2\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountAsSecondJoinModelOuterRefExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (3)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.val
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT counted.val
                        FROM (SELECT 1 AS z) d
                        CROSS JOIN (
                            SELECT count(*) + t0.a AS val
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tval
                            1\t2
                            3\t3
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountBranchArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.*
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) + 2 AS val
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tval
                            1\t4
                            2\t2
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountBranchDistinctProjection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT DISTINCT l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountBranchMultipleSourceCounts() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, 10), (1, NULL)");

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.*
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT counted.other AS cnt_all, counted.*
                            FROM (
                                SELECT min(v) AS other, count(*) AS cnt_all, count(v) AS cnt_v
                                FROM t2
                                WHERE t2.x = t0.a
                            ) counted
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt_all\tother\tcnt_all1\tcnt_v
                            1\t10\t10\t2\t1
                            2\tnull\tnull\t0\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.*
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT counted.other AS cnt_all, counted.*
                            FROM (
                                SELECT min(v) AS other, count(*) AS cnt_all, count(v) AS cnt_v
                                FROM t2
                                WHERE t2.x = t0.a
                            ) counted
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt_all\tother\tcnt_all1\tcnt_v
                            1\t10\t10\t2\t1
                            2\tnull\tnull\t0\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountBranchProjectionWildcardAliasCollisions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, 10)");

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.*
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT counted.other AS cnt, counted.*
                            FROM (
                                SELECT min(v) AS other, count(*) AS cnt
                                FROM t2
                                WHERE t2.x = t0.a
                            ) counted
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt\tother\tcnt1
                            1\t10\t10\t1
                            2\tnull\tnull\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.*
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT *
                            FROM (
                                SELECT counted.other AS cnt, counted.*
                                FROM (
                                    SELECT min(v) AS other, count(*) AS cnt
                                    FROM t2
                                    WHERE t2.x = t0.a
                                ) counted
                            ) projected
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt\tother\tcnt1
                            1\t10\t10\t1
                            2\tnull\tnull\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.*
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT "count.ed".other AS "c.dot", "count.ed".*
                            FROM (
                                SELECT min(v) AS other, count(*) AS "c.dot"
                                FROM t2
                                WHERE t2.x = t0.a
                            ) "count.ed"
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tc.dot\tother\tc.dot1
                            1\t10\t10\t1
                            2\tnull\tnull\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountBranchProjectionWildcardAliasLessWrapper() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT *
                            FROM (
                                SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                            )
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountBranchProjectionWildcardDottedSourceIdentity() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, 10)");

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT "l.2".*
                        FROM t1 l
                        CROSS JOIN LATERAL (
                            SELECT counted.other AS cnt, counted.*
                            FROM (
                                SELECT min(v) AS other, count(*) AS cnt
                                FROM t2
                                WHERE t2.x = t0.a
                            ) counted
                        ) "l.2"
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt\tother\tcnt1
                            1\t10\t10\t1
                            2\tnull\tnull\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountBranchProjectionWildcardInputAliasBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT, v INT, cnt INT)");
            execute("INSERT INTO t2 VALUES (1, 10, 99)");

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.*
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT counted.other AS cnt, counted.*
                            FROM (
                                SELECT min(v) AS other, count(*) AS cnt
                                FROM t2 counted
                                WHERE counted.x = t0.a
                            ) counted
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt\tother\tcnt1
                            1\t10\t10\t1
                            2\tnull\tnull\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountBranchProjectionWrapper() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT cnt
                            FROM (
                                SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                            ) counted
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.c2
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.c2
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT cnt AS c2
                            FROM (
                                SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                            ) counted
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tc2
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.c3, l1.c4
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.c3, l2.c4
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT wrapped.c2 AS c3, wrapped.c2 AS c4
                            FROM (
                                SELECT cnt AS c2
                                FROM (
                                    SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                                ) counted
                            ) wrapped
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tc3\tc4
                            1\t1\t1
                            2\t0\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT *
                            FROM (
                                SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                            ) counted
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT counted.*
                            FROM (
                                SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                            ) counted
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountCorrelatedBranchesAligned() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (7)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");
            execute("CREATE TABLE t3 (x INT, v INT)");
            execute("INSERT INTO t3 VALUES (1, 10), (2, 20)");

            assertQuery("""
                    SELECT t0.a, l1.v, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l3.v, l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT v FROM t3 WHERE t3.x = t0.a
                        ) l3
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a, l1.v
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tv\tcnt
                            1\t10\t1
                            2\t20\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountCorrelatedBranchesAlignedVariants() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 11), (2, 22), (2, 22)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (7)");
            execute("CREATE TABLE t2 (x INT, y INT)");
            execute("INSERT INTO t2 VALUES (1, 11)");
            execute("CREATE TABLE t3 (x INT, y INT, v INT)");
            execute("INSERT INTO t3 VALUES (1, 11, 10), (2, 22, 20)");
            execute("CREATE TABLE t4 (x INT, y INT, w INT)");
            execute("INSERT INTO t4 VALUES (1, 11, 100), (2, 22, 200)");
            execute("CREATE TABLE t5 (x INT, y INT)");
            execute("INSERT INTO t5 VALUES (2, 22)");

            assertQuery("""
                    SELECT t0.a, l1.v, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l3.v, l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT v FROM t3 WHERE t3.x = t0.a
                        ) l3
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a, l1.v
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tv\tcnt
                            1\t10\t1
                            2\t20\t0
                            2\t20\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.v, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l3.v, l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT v FROM t3 WHERE t3.x = t0.a AND v = 10
                        ) l3
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a, l1.v
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tv\tcnt
                            1\t10\t1
                            2\tnull\tnull
                            2\tnull\tnull
                            """);

            assertQuery("""
                    SELECT t0.a, l1.v, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l3.v, l2.cnt
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT v FROM t3 WHERE t3.x = t0.a AND v = 10
                        ) l3
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a, l1.v
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tv\tcnt
                            1\t10\t1
                            2\tnull\t0
                            2\tnull\t0
                            """);

            assertQuery("""
                    SELECT t0.a, t0.b, l1.v, l1.w, l1.cnt, l1.cnt2
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l3.v, l4.w, l2.cnt, l5.cnt2
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a AND t2.y = t0.b
                        ) l2
                        CROSS JOIN LATERAL (
                            SELECT v
                            FROM t3
                            WHERE t3.x = t0.a AND t3.y = t0.b
                        ) l3
                        CROSS JOIN LATERAL (
                            SELECT w
                            FROM t4
                            WHERE t4.x = t0.a AND t4.y = t0.b
                        ) l4
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt2
                            FROM t5
                            WHERE t5.x = t0.a AND t5.y = t0.b
                        ) l5
                    ) l1
                    ORDER BY t0.a, t0.b, l1.v, l1.w
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tb\tv\tw\tcnt\tcnt2
                            1\t11\t10\t100\t1\t0
                            2\t22\t20\t200\t0\t1
                            2\t22\t20\t200\t0\t1
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountDataSourceAliasCollision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (7)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");
            execute("CREATE TABLE t3 (v INT)");
            execute("INSERT INTO t3 VALUES (9)");

            assertQuery("""
                    SELECT t0.a, l1.k, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT __qdb_count_driver__1.k AS k, l2.cnt
                        FROM t1 __qdb_count_driver__1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tcnt
                            1\t7\t1
                            2\t7\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.k, l1.v, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT
                            __qdb_count_driver__2.k AS k,
                            __qdb_count_driver__1.v AS v,
                            l2.cnt
                        FROM t1 __qdb_count_driver__2
                        CROSS JOIN t3 __qdb_count_driver__1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tv\tcnt
                            1\t7\t9\t1
                            2\t7\t9\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountDirectBodyAliasProjection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, 10)");

            assertQuery("""
                    SELECT t0.a, l1.c2
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt AS c2
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tc2
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.c3, l1.c4
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT c2 AS c3, c2 AS c4
                        FROM (
                            SELECT cnt AS c2
                            FROM (
                                SELECT count(*) AS cnt
                                FROM t2
                                WHERE t2.x = t0.a
                            ) counted
                        ) renamed
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tc3\tc4
                            1\t1\t1
                            2\t0\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT counted.other AS cnt_all, counted.*
                        FROM (
                            SELECT min(v) AS other, count(*) AS cnt_all, count(v) AS cnt_v
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tcnt_all\tother\tcnt_all1\tcnt_v
                            1\t10\t10\t1\t1
                            2\tnull\tnull\t0\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.c2
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt AS c2
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                            LIMIT 0
                        ) counted
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tc2
                            1\tnull
                            2\tnull
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountDirectBodyCardinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, NULL)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM (
                            SELECT x FROM t2 WHERE t2.x = t0.a
                        ) s
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM (
                            SELECT x, count(*) AS inner_cnt
                            FROM t2
                            WHERE t2.x = t0.a
                            GROUP BY x
                        ) s
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.cnt, l1.cnt_v
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt, count(v) AS cnt_v
                        FROM t2
                        WHERE t2.x = t0.a
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt\tcnt_v
                            1\t1\t0
                            2\t0\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM t2
                        WHERE t2.x = t0.a
                        LIMIT 0
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\tnull
                            2\tnull
                            """);

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        WHERE cnt > 1
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\tnull
                            2\tnull
                            """);

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM t2
                        WHERE t2.x = t0.a
                        GROUP BY x
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\tnull
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountDirectBodyExactSourceExpressions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE u (cnt INT)");
            execute("INSERT INTO u VALUES (NULL)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, NULL)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (10)");
            execute("CREATE TABLE empty_t1 (k INT)");

            assertQuery("""
                    SELECT t0.a, l1.*, cnt + 1 AS unrelated_plus_one
                    FROM t0
                    CROSS JOIN u
                    LEFT JOIN LATERAL (
                        SELECT cnt AS c2
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tc2\tunrelated_plus_one
                            1\t1\tnull
                            2\t0\tnull
                            """);

            assertQuery("""
                    SELECT t0.a, l1.*,
                           l1.c2 + 1 AS arithmetic,
                           CASE WHEN l1.c2 = 0 THEN 'empty' ELSE 'matched' END AS count_case,
                           COALESCE(l1.c2, -1) AS count_coalesced,
                           l1.c2 + l1.c2 AS repeated
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt AS c2
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tc2\tarithmetic\tcount_case\tcount_coalesced\trepeated
                            1\t1\t2\tmatched\t1\t2
                            2\t0\t1\tempty\t0\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.c3 + 1 AS arithmetic, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT c2 AS c3
                        FROM (
                            SELECT cnt AS c2
                            FROM (
                                SELECT count(*) AS cnt
                                FROM t2
                                WHERE t2.x = t0.a
                            ) counted
                        ) renamed
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tarithmetic\tc3
                            1\t2\t1
                            2\t1\t0
                            """);

            assertQuery("""
                    SELECT t0.a, u.cnt + 1 AS unrelated,
                           l1.cnt_all + 1 AS count_all_plus_one,
                           l1.cnt_v + 1 AS count_v_plus_one
                    FROM t0
                    CROSS JOIN u
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt_all, count(v) AS cnt_v
                        FROM t2
                        WHERE t2.x = t0.a
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tunrelated\tcount_all_plus_one\tcount_v_plus_one
                            1\tnull\t2\t1
                            2\tnull\t1\t1
                            """);

            assertQuery("""
                    SELECT t0.a, l1.c2, l1.c2 + 1 AS arithmetic
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.c2
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT cnt AS c2
                            FROM (
                                SELECT count(*) AS cnt
                                FROM t2
                                WHERE t2.x = t0.a
                            ) counted
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tc2\tarithmetic
                            1\t1\t2
                            2\t0\t1
                            """);

            assertQuery("""
                    SELECT t0.a, l1.c2, l1.c2 + 1 AS arithmetic
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.c2
                        FROM empty_t1
                        CROSS JOIN LATERAL (
                            SELECT cnt AS c2
                            FROM (
                                SELECT count(*) AS cnt
                                FROM t2
                                WHERE t2.x = t0.a
                            ) counted
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tc2\tarithmetic
                            1\tnull\tnull
                            2\tnull\tnull
                            """);

            assertQuery("""
                    SELECT l1.coalesce
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS coalesce
                        FROM t2
                        WHERE t2.x = t0.a
                    ) l1
                    """)
                    .noRandomAccess()
                    .returns("""
                            coalesce
                            1
                            0
                            """);

            assertQuery("""
                    SELECT l1.coalesce + 1 AS arithmetic
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS coalesce
                        FROM t2
                        WHERE t2.x = t0.a
                    ) l1
                    """)
                    .noRandomAccess()
                    .returns("""
                            arithmetic
                            2
                            1
                            """);

            assertQuery("""
                    SELECT t0.a, l1.coalesce, l1.coalesce + 1 AS arithmetic
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS coalesce
                        FROM t2
                        WHERE t2.x = t0.a
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tcoalesce\tarithmetic
                            1\t1\t2
                            2\t0\t1
                            """);

            assertQuery("""
                    SELECT t0.a, l1.c2, l1.c2 + 1 AS arithmetic
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt AS c2
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                            LIMIT 0
                        ) counted
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tc2\tarithmetic
                            1\tnull\tnull
                            2\tnull\tnull
                            """);

            assertQuery("""
                    SELECT t0.a, l1.c2, l1.c2 + 1 AS arithmetic
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt AS c2
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        WHERE cnt > 1
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tc2\tarithmetic
                            1\tnull\tnull
                            2\tnull\tnull
                            """);

            assertQuery("""
                    SELECT q.a, q.c2, q.c2 + 1 AS arithmetic
                    FROM (
                        SELECT t0.a, l1.c2
                        FROM t0
                        LEFT JOIN LATERAL (
                            SELECT count(*) AS c2
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l1
                    ) q
                    ORDER BY q.a
                    """)
                    .returns("""
                            a\tc2\tarithmetic
                            1\t1\t2
                            2\t0\t1
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountDirectBodyMixedWildcardExpressions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.*, l1.cnt + 1 AS cnt_plus_one
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM t2
                        WHERE t2.x = t0.a
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tcnt\tcnt_plus_one
                            1\t1\t2
                            2\t0\t1
                            """);

            assertQuery("""
                    SELECT t0.a, l1.*,
                           CASE WHEN l1.cnt = 0 THEN 'empty' ELSE 'matched' END AS cnt_case,
                           COALESCE(l1.cnt, -1) AS cnt_coalesced
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM t2
                        WHERE t2.x = t0.a
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tcnt\tcnt_case\tcnt_coalesced
                            1\t1\tmatched\t1
                            2\t0\tempty\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.*, l1.cnt + 1 AS cnt_plus_one
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM t2
                        WHERE t2.x = t0.a
                        LIMIT 0
                    ) l1
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tcnt\tcnt_plus_one
                            1\tnull\tnull
                            2\tnull\tnull
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountDirectBodyWindowJoinArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t0 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T00:01:00.000000Z')
                    """);
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");
            execute("CREATE TABLE q (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO q VALUES
                    (10, '2024-01-01T00:00:00.000000Z'),
                    (20, '2024-01-01T00:01:00.000000Z')
                    """);

            assertQuery("""
                    SELECT t0.a, l1.arithmetic, sum(q.v) AS window_sum
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM t2
                        WHERE t2.x = t0.a
                    ) l1
                    WINDOW JOIN q
                        RANGE BETWEEN 0 SECONDS PRECEDING AND CURRENT ROW
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tarithmetic\twindow_sum
                            1\t3\t10
                            2\t2\t20
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountDirectBodyWindowJoinExpressions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t0 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T00:01:00.000000Z')
                    """);
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");
            execute("CREATE TABLE q (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO q VALUES
                    (10, '2024-01-01T00:00:00.000000Z'),
                    (20, '2024-01-01T00:01:00.000000Z')
                    """);

            assertQuery("""
                    SELECT t0.a, l1.c2, l1.c2 + 1 AS arithmetic, sum(q.v) AS window_sum
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt AS c2
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                    ) l1
                    WINDOW JOIN q
                        RANGE BETWEEN 0 SECONDS PRECEDING AND CURRENT ROW
                    ORDER BY t0.a
                    """)
                    .returns("""
                            a\tc2\tarithmetic\twindow_sum
                            1\t1\t2\t10
                            2\t0\t1\t20
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountDriverKeyCombinedAliasAndColumnCollision() throws Exception {
        // T5: user table alias occupies __qdb_count_driver__1, so the driver
        // allocator skips to __qdb_count_driver__2, whose derived key collides
        // with a physical sibling column __qdb_count_driver__2_a
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT, __qdb_count_driver__2_a INT)");
            execute("INSERT INTO t1 VALUES (7, 70)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.k, l1.w, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT
                            __qdb_count_driver__1.k AS k,
                            __qdb_count_driver__1.__qdb_count_driver__2_a AS w,
                            l2.cnt
                        FROM t1 __qdb_count_driver__1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tw\tcnt
                            1\t7\t70\t1
                            2\t7\t70\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountDriverKeyMultiBranchCollision() throws Exception {
        // T7: two correlated scalar-count branches share one driver whose key
        // name collides with a physical sibling column
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (__qdb_count_driver__1_a INT)");
            execute("INSERT INTO t1 VALUES (7)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");
            execute("CREATE TABLE t3 (y INT)");
            execute("INSERT INTO t3 VALUES (2)");

            assertQuery("""
                    SELECT t0.a, l1.v, l1.cnt, l1.cnt2
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.__qdb_count_driver__1_a AS v, l2.cnt, l3.cnt2
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt2 FROM t3 WHERE t3.y = t0.a
                        ) l3
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tv\tcnt\tcnt2
                            1\t7\t1\t0
                            2\t7\t0\t1
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountDriverKeyMultiKeyCollision() throws Exception {
        // T6: two correlation keys; the physical sibling column collides with
        // one of the two derived driver key names
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE t1 (__qdb_count_driver__1_b INT)");
            execute("INSERT INTO t1 VALUES (7)");
            execute("CREATE TABLE t2 (x INT, y INT)");
            execute("INSERT INTO t2 VALUES (1, 10)");

            assertQuery("""
                    SELECT t0.a, t0.b, l1.v, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.__qdb_count_driver__1_b AS v, l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a AND t2.y = t0.b
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tb\tv\tcnt
                            1\t10\t7\t1
                            2\t20\t7\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountDriverKeyPhysicalColumnSibling() throws Exception {
        // T1: a physical sibling column named exactly like the generated
        // driver key must not make the rewritten join criteria ambiguous;
        // the column is not projected inside the lateral body
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (__qdb_count_driver__1_a INT)");
            execute("INSERT INTO t1 VALUES (7)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);

            // alias-less lateral branch: the branch operand of the alignment
            // criteria stays bare, but its clone-prefixed name cannot collide
            // with the driver key namespace
            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        )
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountDriverKeyProjectedAsUserAlias() throws Exception {
        // T2: the v8 review repro; the physical column named like the driver
        // key is projected under a user alias, so basename matching would
        // silently redirect the join key to the user column
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (__qdb_count_driver__1_a INT)");
            execute("INSERT INTO t1 VALUES (7)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.v, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.__qdb_count_driver__1_a AS v, l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tv\tcnt
                            1\t7\t1
                            2\t7\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountDriverKeyQuotedColumnCollision() throws Exception {
        // T10: quoted creation and quoted references of the colliding column
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (\"__qdb_count_driver__1_a\" INT)");
            execute("INSERT INTO t1 VALUES (7)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.v, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1."__qdb_count_driver__1_a" AS v, l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tv\tcnt
                            1\t7\t1
                            2\t7\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountDriverKeyUpperCaseCollision() throws Exception {
        // T9: upper-case physical column; case-insensitive basename matching
        // would capture it even though the spelling differs in case
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (\"__QDB_COUNT_DRIVER__1_A\" INT)");
            execute("INSERT INTO t1 VALUES (7)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.v, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1."__QDB_COUNT_DRIVER__1_A" AS v, l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tv\tcnt
                            1\t7\t1
                            2\t7\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountDriverKeyWrapperLayerCollision() throws Exception {
        // T8: an intermediate projection layer between the lateral top and the
        // join level aliases a user column exactly like the driver key, so the
        // inserted generated key must dedupe and the deduped alias must chain
        // upward into the final alignment criteria
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (7)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.v, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT __qdb_count_driver__1_a AS v, cnt
                        FROM (
                            SELECT t1.k AS __qdb_count_driver__1_a, l2.cnt
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                            ) l2
                        ) inner1
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tv\tcnt
                            1\t7\t1
                            2\t7\t0
                            """);

            // bare-wildcard top over the same colliding wrapper layer: the
            // early-out path must pass the deduped key through the wildcard
            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT *
                        FROM (
                            SELECT t1.k AS __qdb_count_driver__1_a, l2.cnt
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                            ) l2
                        ) inner1
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\t__qdb_count_driver__1_a\tcnt
                            1\t7\t1
                            2\t7\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountEmptyDrivingRelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\tnull
                            2\tnull
                            """);

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.cnt
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\tnull
                            2\tnull
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountInternalAliasWildcardVisibility() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (7)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT
                            t1.k AS __qdb_count_driver__,
                            t1.k AS __qdb_count_driver__user,
                            t1.k AS __qdb_count_driver__1_a,
                            t1.k AS __qdb_outer_ref__,
                            t1.k AS __qdb_outer_ref__user,
                            t1.k AS __qdb_outer_ref__0_a,
                            l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\t__qdb_count_driver__\t__qdb_count_driver__user\t__qdb_count_driver__1_a\t__qdb_outer_ref__\t__qdb_outer_ref__user\t__qdb_outer_ref__0_a\tcnt
                            1\t7\t7\t7\t7\t7\t7\t1
                            2\t7\t7\t7\t7\t7\t7\t0
                            """);

            assertQuery("""
                    SELECT *
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT
                            t1.k AS __qdb_count_driver__,
                            t1.k AS __qdb_count_driver__user,
                            t1.k AS __qdb_count_driver__1_a,
                            t1.k AS __qdb_outer_ref__,
                            t1.k AS __qdb_outer_ref__user,
                            t1.k AS __qdb_outer_ref__0_a,
                            l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\t__qdb_count_driver__\t__qdb_count_driver__user\t__qdb_count_driver__1_a\t__qdb_outer_ref__\t__qdb_outer_ref__user\t__qdb_outer_ref__0_a\tcnt
                            1\t7\t7\t7\t7\t7\t7\t1
                            2\t7\t7\t7\t7\t7\t7\t0
                            """);
        });
    }

    // The intermediate body carries templates transferred by the inner lateral's
    // decorrelation; the LIMIT rewrite must keep them on the wrapped model, where
    // the l2 join alias is still visible.
    @Test
    public void testNestedLateralLeftCountLimitBody() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2), (3)");
            execute("CREATE TABLE t1 (k INT, cnt INT)");
            execute("INSERT INTO t1 VALUES (1, null), (2, 7)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt, l1.c2
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.cnt AS cnt, l2.cnt AS c2
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t1.k
                        ) l2
                        WHERE t1.k = t0.a
                        LIMIT 5
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt\tc2
                            1\tnull\t2
                            2\t7\t0
                            3\tnull\tnull
                            """);

            assertQuery("""
                    SELECT t0.a, l1.v
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.v
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count(*) + 2 AS v FROM t2 WHERE t2.x = t1.k
                        ) l2
                        WHERE t1.k = t0.a
                        LIMIT 5
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tv
                            1\t4
                            2\t2
                            3\tnull
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountLimitCardinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (7)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM t2
                        WHERE t2.x = t0.a
                        LIMIT 1
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.k, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                            LIMIT 0
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tcnt
                            1\tnull\tnull
                            2\tnull\tnull
                            """);

            assertQuery("""
                    SELECT t0.a, l1.k, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.cnt
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                            LIMIT 0
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tcnt
                            1\t7\tnull
                            2\t7\tnull
                            """);

            assertQuery("""
                    SELECT t0.a, l1.k, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tcnt
                            1\t7\t1
                            2\t7\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.k, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                            LIMIT 1
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tcnt
                            1\t7\t1
                            2\t7\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.k, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                            LIMIT 1, 2
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tcnt
                            1\tnull\tnull
                            2\tnull\tnull
                            """);

            // LIMIT reading an outer column (t0.a) cannot be guarded: the guard is
            // evaluated in the outer projection, which does not carry that column.
            // Answering NULL unconditionally would be wrong whenever the window keeps
            // the row (a = 0 gives LIMIT 0,1, where a no-match outer row must yield 0).
            assertException("""
                    SELECT t0.a, l1.k, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                            LIMIT t0.a, t0.a + 1
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """, 202, "LIMIT referencing an outer column is not supported over a scalar count");

            assertQuery("""
                    SELECT t0.a, l1.k, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.cnt
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT cnt
                            FROM (
                                SELECT count(*) AS cnt
                                FROM t2
                                WHERE t2.x = t0.a
                            ) counted
                            WHERE cnt > 1
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tcnt
                            1\t7\tnull
                            2\t7\tnull
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountMultipleDrivingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (10), (20)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.k, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a, l1.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tcnt
                            1\t10\t1
                            1\t20\t1
                            2\t10\t0
                            2\t20\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.k, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k, l2.cnt
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a, l1.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tcnt
                            1\t10\t1
                            1\t20\t1
                            2\t10\t0
                            2\t20\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountOuterRefKeyPhysicalColumnCharacterization() throws Exception {
        // T11 (characterization, pre-existing boundary): a physical column
        // named like an ordinary __qdb_outer_ref__ clone key travels the same
        // wrapper path as the driver key; the identity-tracked propagation
        // hardens the wrapper insertion, while in-branch bare outer-ref
        // references remain HEAD-pre-existing behavior (S1/C1 scope ruling)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (__qdb_outer_ref__0_a INT)");
            execute("INSERT INTO t1 VALUES (7)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.v, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.__qdb_outer_ref__0_a AS v, l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tv\tcnt
                            1\t7\t1
                            2\t7\t0
                            """);
        });
    }

    // Three lateral levels: each enclosing round rewrites the template leaf it
    // owns, so the count body's t0 reference resolves through two clone hops.
    @Test
    public void testNestedLateralLeftCountOuterRefThreeLevels() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t1b (m INT)");
            execute("INSERT INTO t1b VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, l1.val
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.val
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT l3.val
                            FROM t1b
                            LEFT JOIN LATERAL (
                                SELECT count(*) + t0.a AS val
                                FROM t2
                                WHERE t2.x = t0.a
                            ) l3
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tval
                            1\t3
                            2\t2
                            """);
        });
    }

    // The compensation template captures t0.a two lateral levels up; the enclosing
    // lateral's push-down rewrites the template leaf to its outer-ref clone column,
    // so the compensation resolves inside the receiving body.
    @Test
    public void testNestedLateralLeftCountOuterRefTwoLevels() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, l1.val
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.val
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count(*) + t0.a AS val
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tval
                            1\t3
                            2\t2
                            """);
        });
    }

    // An empty middle body yields no rows at all: the compensation inside it has
    // nothing to apply to and the outer row keeps its LEFT JOIN NULL.
    @Test
    public void testNestedLateralLeftCountOuterRefTwoLevelsEmptyMiddle() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, l1.val
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.val
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count(*) + t0.a AS val
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tval
                            1\tnull
                            2\tnull
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountOuterRefTwoLevelsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (NULL)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.val
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.val
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count(*) + t0.a AS val
                            FROM t2
                            WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            a\tval
                            null\tnull
                            """);
        });
    }

    // The same two-level capture through the wildcard/marker path: the rewritten
    // template leaf resolves via marker provenance in the receiving body scope.
    @Test
    public void testNestedLateralLeftCountOuterRefTwoLevelsWildcard() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.*
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT c.* FROM (
                                SELECT count(*) + t0.a AS val
                                FROM t2
                                WHERE t2.x = t0.a
                            ) c
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tval
                            1\t3
                            2\t2
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountQualifiedWildcardAliasCollisions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (NULL)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");
            execute("CREATE TABLE t3 (k INT, cnt INT)");
            execute("INSERT INTO t3 VALUES (NULL, NULL)");

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k AS cnt, l2.*
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt\tcnt1
                            1\tnull\t1
                            2\tnull\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t3.*, l2.*
                        FROM t3
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tk\tcnt\tcnt1
                            1\tnull\tnull\t1
                            2\tnull\tnull\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k AS cnt, t1.k AS cnt1, l2.*
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt\tcnt1\tcnt2
                            1\tnull\tnull\t1
                            2\tnull\tnull\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.*, t1.k AS cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt\tcnt1
                            1\t1\tnull
                            2\t0\tnull
                            """);

            assertQuery("""
                    SELECT t0.a, l1.*
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.k AS cnt, l2.*
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt\tcnt1
                            1\tnull\t1
                            2\tnull\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountQualifiedWildcardExcludes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (NULL)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.*, t1.k AS cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        )
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\tnull
                            2\tnull
                            """);

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.*, t1.k AS cnt
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        )
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\tnull
                            2\tnull
                            """);

            assertQuery("""
                    SELECT t0.a, l1.c2, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT t1.*, l2.cnt AS c2, t1.k AS cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tc2\tcnt
                            1\t1\tnull
                            2\t0\tnull
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountQualifiedWildcardRepeatedProjection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt AS c1, l1.cnt AS c2
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.*
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tc1\tc2
                            1\t1\t1
                            2\t0\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountQualifiedWildcardSurfaces() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.*
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT "Count.Source".*
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) "Count.Source"
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t1
                            2\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountQuotedDottedAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, l1."c.dot"
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2."c.dot"
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS "c.dot" FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tc.dot
                            1\t1
                            2\t0
                            """);
        });
    }

    // Nested LEFT lateral over an intermediate CROSS lateral count: the inner
    // count subquery always yields exactly one row, so l1 yields one row per
    // outer row and the LEFT join must produce cnt=0, not NULL, for outer rows
    // without a match. The count column surfaces through the intermediate
    // lateral projection, so the coalesce compensation must trace it through
    // the l2 branch.
    @Test
    public void testNestedLateralLeftCountSkipLevelQualified() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, t0.b, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tb\tcnt
                            1\t10\t1
                            2\t20\t0
                            """);
        });
    }

    // The intermediate lateral projection renames the count column
    // (l2.cnt AS c2); the coalesce compensation must follow the rename when
    // wrapping the outer reference l1.c2.
    @Test
    public void testNestedLateralLeftCountSkipLevelRenamed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, t0.b, l1.c2
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.cnt AS c2
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tb\tc2
                            1\t10\t1
                            2\t20\t0
                            """);
        });
    }

    // Same shape as testNestedLateralLeftCountSkipLevelQualified, but the
    // innermost correlated reference to t0.a is unqualified, so the rewriter
    // must recognize it through its lateralDepth tag.
    @Test
    public void testNestedLateralLeftCountSkipLevelUnqualified() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, t0.b, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.cnt
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tb\tcnt
                            1\t10\t1
                            2\t20\t0
                            """);
        });
    }

    // C1 guards, the other direction. The compensation must stay suppressed
    // wherever the wrapper provably rejects the zero row, so these pin NULL as
    // the right answer and fail if the classifier ever turns permissive.
    @Test
    public void testNestedLateralLeftCountWrapperFilterConjunctionDropsZeroRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            // cnt >= 0 holds at 0 but cnt > 1 does not, and FALSE AND TRUE is FALSE
            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        WHERE cnt >= 0 AND cnt > 1
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t2
                            2\tnull
                            """);
        });
    }

    // C1 - a lateral count() body under a wrapper layer that provably KEEPS the
    // aggregate row. count() with no GROUP BY over an empty input is one implicit
    // group, so the body emits one row holding 0 even for an unmatched outer row.
    // Every predicate/layer below is satisfied by that zero row, so the body is
    // non-empty and LEFT JOIN must not NULL-fill: SQL requires 0, not NULL.
    //
    // The existing wrapper-predicate coverage in this file is WHERE cnt > 1 at
    // three sites, the one predicate that is FALSE at 0 - i.e. the only shape
    // where NULL is right. These assert the complementary, keeps-the-row half.
    @Test
    public void testNestedLateralLeftCountWrapperFilterDisjunctionCoversZero() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        WHERE cnt = 0 OR cnt > 0
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t2
                            2\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountWrapperFilterInequalityDropsZeroRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        WHERE cnt <> 0
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t2
                            2\tnull
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountWrapperFilterKeepsZeroRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        WHERE cnt >= 0
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t2
                            2\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountWrapperFilterLowerBoundAcceptsWholeDomain() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            // count() is non-negative, so cnt > -1 cannot reject anything
            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        WHERE cnt > -1
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t2
                            2\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountWrapperFilterTautology() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        WHERE 1 = 1
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t2
                            2\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountWrapperGroupByCountOutput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            // GROUP BY above the aggregate groups the single zero row by its own
            // value: one group in, one group out. The row survives.
            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        GROUP BY cnt
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t2
                            2\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountWrapperJoinKeepsZeroRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            // count body as the FIRST join model. The companion shape with the
            // operands swapped is asserted by
            // testNestedLateralLeftCountAsSecondJoinModel and already returns 0:
            // the two must agree, a CROSS JOIN d and d CROSS JOIN a are the same
            // relation.
            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT counted.cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        CROSS JOIN (SELECT 1 AS z) d
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t2
                            2\t0
                            """);
        });
    }

    // A filter that rejects part of the count domain cannot stay inside the body:
    // once it removes a group, that group is indistinguishable at the outer
    // projection from one that never existed. Such a filter is lifted into the
    // guard, where it judges the compensated value instead, and both cells come
    // out right - the matched row the filter rejects becomes NULL, the unmatched
    // row the filter accepts becomes 0. Fails if the lift regresses to either
    // half: suppressing gives NULL for the unmatched row, compensating without
    // the lift gives a fabricated 0 for the rejected one.
    @Test
    public void testNestedLateralLeftCountWrapperNegatedFilterLifted() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        WHERE NOT (cnt > 1)
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\tnull
                            2\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountWrapperNonTotalFilterLifted() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            // cnt < 2 accepts the zero row but rejects a count of 2
            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        WHERE cnt < 2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\tnull
                            2\t0
                            """);
        });
    }

    // A lifted wrapper filter and a bind-variable LIMIT compose into one guard:
    // the compensated value must pass the filter AND the limit must keep row 1.
    @Test
    public void testNestedLateralLeftCountWrapperNonTotalFilterLiftedBindVarLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            String sql = """
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        WHERE cnt < 1
                        LIMIT :lim
                    ) l1
                    ORDER BY t0.a
                    """;

            bindVariableService.setLong("lim", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    a\tcnt
                    1\tnull
                    2\t0
                    """);

            bindVariableService.setLong("lim", 0);
            assertQuery(sql).noLeakCheck().returns("""
                    a\tcnt
                    1\tnull
                    2\tnull
                    """);

            bindVariableService.setLong("lim", 1);
            assertQuery(sql).noLeakCheck().returns("""
                    a\tcnt
                    1\tnull
                    2\t0
                    """);
        });
    }

    // Lifting a filter is only safe while the body projects nothing but the
    // count: every other column would then be exposed to rows the filter removed,
    // and only the count is compensated. With a second column present the filter
    // must stay inside the body, so the row it rejects stays gone in full.
    @Test
    public void testNestedLateralLeftCountWrapperNonTotalFilterNotLiftedWhenBodyHasOtherColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt, l1.tag
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt, tag
                        FROM (
                            SELECT count(*) AS cnt, 'T' AS tag
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        WHERE cnt < 2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt\ttag
                            1\tnull\t
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountWrapperUnionAllEmptyBranch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            // the second branch is provably empty for every outer row, so the union
            // contributes exactly the first branch: one row per outer row.
            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        UNION ALL
                        SELECT cnt
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted2
                        WHERE cnt > 1000
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t2
                            2\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftCountWrapperWildcardFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT *
                        FROM (
                            SELECT count(*) AS cnt
                            FROM t2
                            WHERE t2.x = t0.a
                        ) counted
                        WHERE cnt >= 0
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tcnt
                            1\t2
                            2\t0
                            """);
        });
    }

    // LEFT lateral over an intermediate LEFT lateral count: the LEFT inner
    // branch takes the join-branch push-down path instead of the per-side
    // push, and must produce the same cnt=0 compensation.
    @Test
    public void testNestedLateralLeftOverLeftCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, t0.b, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.cnt
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count(*) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tb\tcnt
                            1\t10\t1
                            2\t20\t0
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftOverLeftCountArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1)");

            assertQuery("""
                    SELECT t0.a, t0.b, l1.val
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.val
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count(*) + 2 AS val FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tb\tval
                            1\t10\t3
                            2\t20\t2
                            """);
        });
    }

    @Test
    public void testNestedLateralLeftOverLeftCountDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT)");
            execute("INSERT INTO t2 VALUES (1), (1)");

            assertQuery("""
                    SELECT t0.a, t0.b, l1.cnt
                    FROM t0
                    LEFT JOIN LATERAL (
                        SELECT l2.cnt
                        FROM t1
                        LEFT JOIN LATERAL (
                            SELECT count_distinct(x) AS cnt FROM t2 WHERE t2.x = t0.a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tb\tcnt
                            1\t10\t1
                            2\t20\t0
                            """);
        });
    }

    // Adjacent pin: nested CROSS lateral sum with a skip-level unqualified
    // correlated ref. Every outer row matches, so no compensation fires; the
    // decorrelated aggregate must still evaluate per outer row.
    @Test
    public void testNestedLateralSkipLevelAggregate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, 100), (1, 200), (2, 300)");

            assertQuery("""
                    SELECT t0.a, t0.b, l1.s
                    FROM t0
                    CROSS JOIN LATERAL (
                        SELECT l2.s
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT sum(v) AS s FROM t2 WHERE t2.x = a
                        ) l2
                    ) l1
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\ts
                            1\t10\t300
                            2\t20\t300
                            """);
        });
    }

    // Adjacent pin: outer WHERE on the outer table must not disturb the
    // skip-level correlated pushdown into the nested lateral branch.
    @Test
    public void testNestedLateralSkipLevelWherePushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 10), (2, 20), (3, 30)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (x INT, v INT)");
            execute("INSERT INTO t2 VALUES (1, 100), (2, 200), (3, 300)");

            assertQuery("""
                    SELECT t0.a, l1.v
                    FROM t0
                    CROSS JOIN LATERAL (
                        SELECT l2.v
                        FROM t1
                        CROSS JOIN LATERAL (
                            SELECT v FROM t2 WHERE t2.x = a
                        ) l2
                    ) l1
                    WHERE t0.b < 30
                    ORDER BY t0.a
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tv
                            1\t100
                            2\t200
                            """);
        });
    }

    @Test
    public void testOuterRefWildcardExcludedModelLifecycle() {
        QueryModel model = QueryModel.FACTORY.newInstance();
        QueryModelWrapper wrapper = new QueryModelWrapper();
        wrapper.setDelegate(model);

        Assert.assertFalse(model.isLateralCountCoalesceRequired());
        Assert.assertFalse(model.isOuterRefJoinChecked());
        Assert.assertFalse(model.isOuterRefWildcardExcluded());
        Assert.assertFalse(wrapper.isLateralCountCoalesceRequired());
        Assert.assertFalse(wrapper.isOuterRefJoinChecked());
        Assert.assertFalse(wrapper.isOuterRefWildcardExcluded());
        model.setLateralCountCoalesceRequired(true);
        model.setOuterRefJoinChecked(true);
        model.setOuterRefWildcardExcluded(true);
        Assert.assertTrue(model.isLateralCountCoalesceRequired());
        Assert.assertTrue(model.isOuterRefJoinChecked());
        Assert.assertTrue(model.isOuterRefWildcardExcluded());
        Assert.assertTrue(wrapper.isLateralCountCoalesceRequired());
        Assert.assertTrue(wrapper.isOuterRefJoinChecked());
        Assert.assertTrue(wrapper.isOuterRefWildcardExcluded());
        try {
            wrapper.setLateralCountCoalesceRequired(false);
            Assert.fail("QueryModelWrapper must remain read-only");
        } catch (UnsupportedOperationException ignored) {
        }
        try {
            wrapper.setOuterRefJoinChecked(false);
            Assert.fail("QueryModelWrapper must remain read-only");
        } catch (UnsupportedOperationException ignored) {
        }
        try {
            wrapper.setOuterRefWildcardExcluded(false);
            Assert.fail("QueryModelWrapper must remain read-only");
        } catch (UnsupportedOperationException ignored) {
        }

        QueryColumn template = new QueryColumn().of("cnt", null);
        try {
            wrapper.addLateralCountTemplate(template);
            Assert.fail("QueryModelWrapper must remain read-only");
        } catch (UnsupportedOperationException ignored) {
        }
        model.addLateralCountTemplate(template);
        Assert.assertEquals(1, model.getLateralCountTemplates().size());
        Assert.assertSame(template, wrapper.getLateralCountTemplates().getQuick(0));

        model.clear();
        Assert.assertFalse(model.isLateralCountCoalesceRequired());
        Assert.assertFalse(model.isOuterRefJoinChecked());
        Assert.assertFalse(model.isOuterRefWildcardExcluded());
        Assert.assertFalse(wrapper.isLateralCountCoalesceRequired());
        Assert.assertFalse(wrapper.isOuterRefJoinChecked());
        Assert.assertFalse(wrapper.isOuterRefWildcardExcluded());
        Assert.assertEquals(0, wrapper.getLateralCountTemplates().size());
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

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcategory\ttrade_total
                            1\tA\t30.0
                            2\tB\t30.0
                            """);
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

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttrade_total
                            1\t30.0
                            2\t30.0
                            """);
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

            assertQuery("""
                    SELECT t1.id, coalesce(sub.total, 0) AS total
                    FROM t1
                    LEFT JOIN LATERAL (
                        SELECT sum(val) AS total
                        FROM t2
                        WHERE t2.t1_id = t1.id
                    ) sub
                    ORDER BY t1.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ttotal
                            1\t30
                            2\t0
                            3\t0
                            """);
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

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcategory\ttrade_total\treturn_total
                            1\tA\t30.0\t5.0
                            2\tB\t30.0\t15.0
                            """);
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

            assertQuery("""
                    SELECT t1.id, sub.total
                    FROM t1
                    CROSS JOIN LATERAL (
                        SELECT sum(val) AS total
                        FROM t2
                        WHERE t2.t1_id = t1.id
                    ) sub
                    ORDER BY t1.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ttotal
                            1\t30
                            2\t30
                            """);
        });
    }

    @Test
    public void testRepeatedLiteralProjectionSourceLookup() throws Exception {
        assertQuery("""
                SELECT
                    x AS c01, x AS c02, x AS c03, x AS c04,
                    x AS c05, x AS c06, x AS c07, x AS c08,
                    x AS c09, x AS c10, x AS c11, x AS c12,
                    x AS c13, x AS c14, x AS c15, x AS c16
                FROM (SELECT 0 AS k0) s0
                CROSS JOIN (SELECT 1 AS k1) s1
                CROSS JOIN (SELECT 2 AS k2) s2
                CROSS JOIN (SELECT 3 AS k3) s3
                CROSS JOIN (SELECT 4 AS k4) s4
                CROSS JOIN (SELECT 5 AS k5) s5
                CROSS JOIN (SELECT 6 AS k6) s6
                CROSS JOIN (SELECT 42 AS x) s7
                """)
                .expectSize()
                .noRandomAccess()
                .returns("""
                        c01\tc02\tc03\tc04\tc05\tc06\tc07\tc08\tc09\tc10\tc11\tc12\tc13\tc14\tc15\tc16
                        42\t42\t42\t42\t42\t42\t42\t42\t42\t42\t42\t42\t42\t42\t42\t42
                        """);
    }

    @Test
    public void testScalarAggregateBodyExcludesWindowJoinBody() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE instruments (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (instrument_id INT, price DOUBLE, tag SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE quotes (price DOUBLE, tag SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO instruments VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T00:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, 'A', '2024-01-01T00:01:00.000000Z'),
                    (1, 11.0, 'A', '2024-01-01T00:02:00.000000Z')
                    """);
            execute("""
                    INSERT INTO quotes VALUES
                    (9.5, 'A', '2024-01-01T00:00:30.000000Z')
                    """);

            // the WINDOW JOIN aggregates per driving row, so instrument 2 keeps no row at all
            assertQuery("""
                    SELECT i.id, sub.c
                    FROM instruments i
                    JOIN LATERAL (
                        SELECT count(*) AS c
                        FROM trades t
                        WINDOW JOIN quotes q ON tag
                            RANGE BETWEEN 1 MINUTE PRECEDING AND CURRENT ROW
                        WHERE t.instrument_id = i.id
                    ) sub
                    ORDER BY i.id, sub.c
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tc
                            1\t1
                            1\t1
                            """);
        });
    }

    @Test
    public void testScalarCountBodyPreservesCrossLateralRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (order_id INT, venue SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 'LSE', '2024-01-01T00:10:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.id, sub.cnt
                    FROM orders o
                    CROSS JOIN LATERAL (
                        SELECT count(*) AS cnt FROM trades WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcnt
                            1\t1
                            2\t0
                            """);

            assertQuery("""
                    SELECT o.id, sub.m
                    FROM orders o
                    CROSS JOIN LATERAL (
                        SELECT max(ts) AS m FROM trades WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tm
                            1\t2024-01-01T00:10:00.000000Z
                            2\t
                            """);

            assertQuery("""
                    SELECT o.id, sub.m
                    FROM orders o
                    JOIN LATERAL (
                        SELECT max(ts) AS m FROM trades WHERE order_id = o.id
                    ) sub ON true
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tm
                            1\t2024-01-01T00:10:00.000000Z
                            2\t
                            """);
        });
    }

    @Test
    public void testT01ScanInner() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQuery("""
                    SELECT o.id, o.customer, t.qty
                    FROM orders o
                    JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcustomer\tqty
                            1\tAlice\t10.0
                            1\tAlice\t20.0
                            2\tBob\t30.0
                            3\tCharlie\t40.0
                            3\tCharlie\t50.0
                            """);
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

            assertQuery("""
                    SELECT o.id, o.customer, t.qty
                    FROM orders o
                    LEFT JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcustomer\tqty
                            1\tAlice\t10.0
                            1\tAlice\t20.0
                            2\tBob\tnull
                            3\tCharlie\tnull
                            """);
        });
    }

    // T03: GROUP BY + SUM, INNER, equality correlation — GROUP BY expansion
    @Test
    public void testT03GroupByInner() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQuery("""
                    SELECT o.id, o.customer, t.total_qty
                    FROM orders o
                    JOIN LATERAL (SELECT sum(qty) AS total_qty FROM trades WHERE order_id = o.id) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcustomer\ttotal_qty
                            1\tAlice\t30.0
                            2\tBob\t30.0
                            3\tCharlie\t90.0
                            """);
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

            assertQuery("""
                    SELECT o.id, t.cnt
                    FROM orders o
                    LEFT JOIN LATERAL (SELECT count(*) AS cnt FROM trades WHERE order_id = o.id) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcnt
                            1\t2
                            2\t1
                            3\t0
                            """);
        });
    }

    // T05: Window(sum OVER), INNER, equality correlation — PARTITION BY expansion
    @Test
    public void testT05WindowInner() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQuery("""
                    SELECT o.id, t.qty, t.running_total
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty, sum(qty) OVER (ORDER BY ts) AS running_total
                        FROM trades
                        WHERE order_id = o.id
                    ) t
                    WHERE o.id IN (1, 3)
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\trunning_total
                            1\t10.0\t10.0
                            1\t20.0\t30.0
                            3\t40.0\t40.0
                            3\t50.0\t90.0
                            """);
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

            assertQuery("""
                    SELECT m.mm_id, m.symbol, f.qty, f.running_sum
                    FROM mm m
                    JOIN LATERAL (
                        SELECT qty, sum(qty) OVER (ORDER BY ts) AS running_sum
                        FROM fills
                        WHERE mm_id = m.mm_id AND symbol = m.symbol AND qty >= 150.0
                    ) f
                    ORDER BY m.mm_id, f.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            mm_id\tsymbol\tqty\trunning_sum
                            1\tAAPL\t150.0\t350.0
                            1\tAAPL\t200.0\t200.0
                            2\tGOOG\t300.0\t300.0
                            2\tGOOG\t400.0\t700.0
                            """);
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

            assertQuery("""
                    SELECT o.id, o.customer, t.qty
                    FROM orders o
                    JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id ORDER BY qty DESC LIMIT 2) t
                    ORDER BY o.id, t.qty DESC
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcustomer\tqty
                            1\tAlice\t30.0
                            1\tAlice\t20.0
                            2\tBob\t60.0
                            2\tBob\t50.0
                            """);
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

            assertQuery("""
                    SELECT o.id, t.category
                    FROM orders o
                    JOIN LATERAL (SELECT DISTINCT category FROM trades WHERE order_id = o.id) t
                    ORDER BY o.id, t.category
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcategory
                            1\tA
                            1\tB
                            2\tC
                            """);
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

            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades_a WHERE order_id = o.id
                        UNION ALL
                        SELECT qty FROM trades_b WHERE order_id = o.id
                    ) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty
                            1\t10.0
                            1\t30.0
                            2\t20.0
                            2\t40.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.cnt, sub.cnt + 1 AS cnt_plus_one
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt FROM trades WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcnt\tcnt_plus_one
                            1\t2\t3
                            2\t1\t2
                            3\t0\t1
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty, sub.adj
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, a.adj
                        FROM trades t
                        RIGHT JOIN adjustments a ON a.order_id = t.order_id
                        WHERE a.order_id = o.id
                    ) sub
                    ORDER BY o.id, sub.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tadj
                            1\t10.0\t1.5
                            1\t20.0\t1.5
                            2\tnull\t2.0
                            """);
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
            // The rewriter places the outer-ref CROSS JOIN after the FULL JOIN, so the
            // unmatched refund repeats per outer row without relying on the optimiser
            // to reorder the joins.
            assertQuery("""
                    SELECT o.id, sub.qty, sub.amount
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, r.amount
                        FROM trades t
                        FULL OUTER JOIN refunds r ON r.order_id = t.order_id
                        WHERE t.order_id = o.id OR r.order_id = o.id
                    ) sub
                    ORDER BY o.id, sub.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Cross Join\n                            Hash Full Outer Join Light")
                    .returns("""
                            id\tqty\tamount
                            1\t10.0\tnull
                            1\t20.0\tnull
                            2\tnull\t5.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tadj\tdisc
                            2\t30.0\tnull\t0.8
                            """);
        });
    }

    // T100d: the FULL JOIN keeps the unmatched refund once per outer row when only
    // a later LEFT JOIN reads the outer row
    @Test
    public void testT100dFullJoinKeepsUnmatchedRowsWithCorrelatedLaterLeftJoin() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefundsAndXs();

            assertQuery("""
                    SELECT o.id, sub.qty, sub.amount, sub.xv
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, r.amount, x.v xv
                        FROM trades t
                        FULL JOIN refunds r ON r.order_id = t.order_id
                        LEFT JOIN xs x ON x.k = o.id
                    ) sub
                    ORDER BY o.id, sub.qty, sub.amount
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tamount\txv
                            1\t10.0\tnull\t100
                            1\t20.0\tnull\t100
                            1\tnull\t5.0\t100
                            2\t10.0\tnull\tnull
                            2\t20.0\tnull\tnull
                            2\tnull\t5.0\tnull
                            """);
        });
    }

    // T100e: the RIGHT JOIN keeps the unmatched refund once per outer row when only
    // a later LEFT JOIN reads the outer row
    @Test
    public void testT100eRightJoinKeepsUnmatchedRowsWithCorrelatedLaterLeftJoin() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefundsAndXs();

            assertQuery("""
                    SELECT o.id, sub.qty, sub.amount, sub.xv
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, r.amount, x.v xv
                        FROM trades t
                        RIGHT JOIN refunds r ON r.order_id = t.order_id
                        LEFT JOIN xs x ON x.k = o.id
                    ) sub
                    ORDER BY o.id, sub.qty, sub.amount
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tamount\txv
                            1\tnull\t5.0\t100
                            2\tnull\t5.0\tnull
                            """);
        });
    }

    // T100f: the FULL JOIN keeps the unmatched refund for the outer row that the
    // later correlated INNER JOIN accepts
    @Test
    public void testT100fFullJoinKeepsUnmatchedRowsWithCorrelatedLaterInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefundsAndXs();

            assertQuery("""
                    SELECT o.id, sub.qty, sub.amount, sub.xv
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, r.amount, x.v xv
                        FROM trades t
                        FULL JOIN refunds r ON r.order_id = t.order_id
                        JOIN xs x ON x.k = o.id
                    ) sub
                    ORDER BY o.id, sub.qty, sub.amount
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tamount\txv
                            1\t10.0\tnull\t100
                            1\t20.0\tnull\t100
                            1\tnull\t5.0\t100
                            """);
        });
    }

    // T100g: count(*) over a FULL JOIN counts the unmatched refund for every outer
    // row when only a later LEFT JOIN reads the outer row
    @Test
    public void testT100gLeftLateralCountFullJoinWithCorrelatedLaterLeftJoin() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefundsAndXs();

            assertQuery("""
                    SELECT o.id, sub.c
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) c
                        FROM trades t
                        FULL JOIN refunds r ON r.order_id = t.order_id
                        LEFT JOIN xs x ON x.k = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc
                            1\t3
                            2\t3
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id	sum
                            1	19.5
                            1	42.0
                            2	39.0
                            """);
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

            assertQuery("""
                    SELECT i.id, sub.horizon_sec, sub.avg_mid
                    FROM instruments i
                    JOIN LATERAL (
                        SELECT h.offset / 1_000_000 AS horizon_sec,
                               avg((q.bid + q.ask) / 2) AS avg_mid
                        FROM trades t
                        HORIZON JOIN quotes q ON (symbol)
                            LIST (0, 1s) AS h
                        WHERE t.symbol = i.symbol
                        GROUP BY horizon_sec
                    ) sub
                    ORDER BY i.id, sub.horizon_sec
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\thorizon_sec\tavg_mid
                            1\t0\t100.0
                            1\t1\t100.0
                            2\t0\t200.0
                            2\t1\t200.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcount\tavg_mid
                            1\t2\t100.5
                            2\t1\t200.0
                            """);
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

            // MSFT has no trades, so scalar count returns 0 while avg remains NULL
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id	n	avg_mid
                            1	1	100.0
                            2	1	200.0
                            3	0	null
                            """);
        });
    }

    @Test
    public void testT102dLeftLateralHorizonJoinImplicitKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE instruments (id INT, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE quotes (symbol SYMBOL, bid DOUBLE, ask DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO instruments VALUES
                    (1, 'AAPL', '2024-01-01T00:00:00.000000Z'),
                    (2, 'MSFT', '2024-01-01T00:00:00.000000Z')
                    """);
            execute("INSERT INTO trades VALUES ('AAPL', 100.0, '2024-01-01T00:01:00.000000Z')");
            execute("""
                    INSERT INTO quotes VALUES
                    ('AAPL', 99.0, 101.0, '2024-01-01T00:00:30.000000Z'),
                    ('AAPL', 100.0, 102.0, '2024-01-01T00:01:30.000000Z')
                    """);

            assertQuery("""
                    SELECT i.id, sub.horizon_sec, sub.n
                    FROM instruments i
                    LEFT JOIN LATERAL (
                        SELECT h.offset / 1_000_000 AS horizon_sec, count() AS n
                        FROM trades t
                        HORIZON JOIN quotes q ON (symbol)
                            LIST (0, 1s) AS h
                        WHERE t.symbol = i.symbol
                    ) sub ON true
                    ORDER BY i.id, sub.horizon_sec
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\thorizon_sec\tn
                            1\t0\t1
                            1\t1\t1
                            2\tnull\tnull
                            """);
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

            assertQuery("""
                    SELECT o.id, sub.order_id + 1 AS next_id, sub.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT order_id, qty FROM trades WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tnext_id\tqty
                            1\t2\t10.0
                            2\t3\t30.0
                            """);
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

            assertQuery("""
                    SELECT o.id, coalesce(sub.qty, sub.qty2, 0) AS result
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty, qty2 FROM trades WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tresult
                            1\t10.0
                            2\t30.0
                            """);
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
            assertQuery("""
                    SELECT o.id, (sub.qty * 2) + (sub.qty - 1) AS result
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tresult
                            1\t29.0
                            2\t89.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.child_id, sub.child_amount
                    FROM orders o
                    JOIN LATERAL (
                        SELECT id AS child_id, amount AS child_amount
                        FROM orders
                        WHERE parent_id = o.id
                    ) sub
                    ORDER BY o.id, sub.child_id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tchild_id\tchild_amount
                            1\t2\t50.0
                            1\t3\t30.0
                            2\t4\t20.0
                            """);
        });
    }

    @Test
    public void testT105UnsupportedJoinLateralError() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            assertQuery("SELECT * FROM t1 ASOF JOIN LATERAL (SELECT * FROM t2 WHERE x = t1.x) t")
                    .fails(27, "LATERAL is only supported with INNER, LEFT, or CROSS joins");

            assertQuery("SELECT * FROM t1 SPLICE JOIN LATERAL (SELECT * FROM t2 WHERE x = t1.x) t")
                    .fails(29, "LATERAL is only supported with INNER, LEFT, or CROSS joins");

            assertQuery("SELECT * FROM t1 FULL OUTER JOIN LATERAL (SELECT * FROM t2 WHERE x = t1.x) t ON true")
                    .fails(33, "LATERAL is only supported with INNER, LEFT, or CROSS joins");
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
            assertQuery("""
                    SELECT o.id, sub.qty, sub.adj
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, a.adj
                        FROM trades t
                        LEFT JOIN adjustments a ON a.order_id = o.id AND a.order_id = t.order_id
                        WHERE t.order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tadj
                            1\t10.0\t1.5
                            2\t30.0\tnull
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty, sub.adj
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, a.adj
                        FROM trades t
                        RIGHT JOIN adjustments a ON a.order_id = o.id AND a.order_id = t.order_id
                        WHERE a.order_id = o.id
                    ) sub
                    ORDER BY o.id, sub.adj
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tadj
                            1\t10.0\t1.5
                            2\tnull\t2.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty, sub.amount
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, r.amount
                        FROM trades t
                        FULL OUTER JOIN refunds r ON r.order_id = t.order_id
                        WHERE t.order_id = o.id OR r.order_id = o.id
                    ) sub
                    ORDER BY o.id, sub.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tamount
                            1\t10.0\tnull
                            1\t20.0\tnull
                            2\tnull\t5.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tadj
                            1\t10.0\t1.5
                            2\tnull\t2.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty, sub.bonus
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, b.bonus
                        FROM trades t
                        LEFT JOIN bonuses b ON b.trade_order_id = t.order_id AND b.bonus > o.min_qty
                        WHERE t.order_id = o.id
                    ) sub
                    ORDER BY o.id, sub.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tbonus
                            1\t10.0\t100.0
                            1\t20.0\t100.0
                            2\t30.0\t200.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty, sub.adj
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, a.adj
                        FROM (SELECT order_id, qty FROM trades WHERE order_id = o.id) t
                        JOIN adjustments a ON a.order_id = o.id
                    ) sub
                    ORDER BY o.id, sub.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tadj
                            1\t10.0\t1.5
                            1\t20.0\t1.5
                            2\t30.0\t2.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades WHERE order_id = o.id
                        UNION ALL
                        SELECT qty FROM returns WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id, sub.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty
                            1\t5.0
                            1\t10.0
                            2\t30.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades
                        WHERE order_id = o.id AND qty > o.min_qty
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            1\t20.0
                            2\t30.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.category, sub.total
                    FROM orders o
                    JOIN LATERAL (
                        SELECT category, total
                        FROM (SELECT order_id, category, sum(qty) AS total
                              FROM trades GROUP BY order_id, category)
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id, sub.category
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcategory\ttotal
                            1\tA\t10.0
                            1\tB\t20.0
                            2\tA\t70.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcategory\ttotal
                            1\tC\t30.0
                            2\tY\t50.0
                            2\tZ\t60.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades_a WHERE order_id = o.id
                        INTERSECT
                        SELECT qty FROM trades_b WHERE order_id = o.id
                    ) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty
                            1\t10.0
                            2\t30.0
                            """);
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

            assertQuery("""
                    SELECT count(*) AS count
                    FROM large_orders o
                    JOIN LATERAL (
                        SELECT qty FROM large_trades WHERE order_id = o.id ORDER BY qty LIMIT 3
                    ) t
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            3000
                            """);

            // LEFT JOIN with GROUP BY: every order gets a sum
            assertQuery("""
                    SELECT count(*) AS count
                    FROM large_orders o
                    LEFT JOIN LATERAL (
                        SELECT sum(qty) AS total FROM large_trades WHERE order_id = o.id
                    ) t
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            1000
                            """);
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

            assertQuery("""
                    SELECT o.id, sub.qty, sub.fee
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, f.fee
                        FROM (SELECT qty, order_id FROM trades WHERE order_id = o.id) t
                        JOIN (SELECT fee, order_id FROM fees WHERE order_id = o.id) f
                            ON f.order_id = t.order_id
                    ) sub
                    ORDER BY o.id, sub.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tfee
                            1\t10.0\t0.5
                            1\t20.0\t0.5
                            2\t30.0\t1.0
                            """);
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

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tfee\tdisc
                            1\t10.0\t0.5\t0.1
                            2\t30.0\t1.0\t0.2
                            """);
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

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tmax_qty
                            1\t10.0\t100.0
                            2\t30.0\t200.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty
                        FROM trades
                        WHERE order_id = o.id
                        LATEST ON ts PARTITION BY order_id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            1\t20.0
                            2\t40.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty, sub.label
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, g.label
                        FROM trades t
                        JOIN (SELECT id, label FROM tags WHERE order_id = o.id) g ON g.id = t.tag_id
                        WHERE t.order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tlabel
                            1\t10.0\turgent
                            2\t30.0\tnormal
                            """);
        });
    }

    // T113: LATERAL JOIN without explicit alias — triggers lateralAlias==null path
    @Test
    public void testT113LateralJoinNoAlias() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQuery("""
                    SELECT o.id, qty
                    FROM orders o
                    JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id)
                    ORDER BY o.id, qty
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            1\t10.0
                            1\t20.0
                            2\t30.0
                            3\t40.0
                            3\t50.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty\tlevel
                            1\t5.0\tlow
                            1\t15.0\tmid
                            1\t25.0\thigh
                            2\t20.0\tlow
                            2\t30.0\tmid
                            2\t40.0\thigh
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcnt\tlabel
                            1\t2\tmulti
                            2\t1\tsingle
                            3\t0\tnone
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty, sub.rn
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty,
                               row_number() OVER (PARTITION BY o.category ORDER BY ts) AS rn
                        FROM trades
                        WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id, sub.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\trn
                            1\t10.0\t1
                            1\t20.0\t2
                            2\t30.0\t1
                            """);
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

            assertQuery("""
                    SELECT o.id, t.name, t.price
                    FROM orders o
                    JOIN LATERAL (
                        SELECT p.name, pr.price
                        FROM products p
                        JOIN prices pr ON pr.product_id = p.id
                        WHERE p.order_id = o.id
                    ) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tname\tprice
                            1\tWidget\t9.99
                            2\tGadget\t19.99
                            """);
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
            assertQuery("""
                    SELECT o.id, t.category, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT category, qty FROM trades
                        WHERE order_id = o.id
                        LATEST ON ts PARTITION BY category
                    ) t
                    ORDER BY o.id, t.category
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcategory\tqty
                            1\tA\t20.0
                            1\tB\t30.0
                            2\tA\t40.0
                            2\tB\t60.0
                            """);
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

            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades
                        WHERE order_id = o.id AND qty > o.min_qty
                    ) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            1\t20.0
                            1\t30.0
                            2\t30.0
                            """);
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

            assertQuery("""
                    SELECT m.mm_id, m.symbol, t.total
                    FROM master m
                    LEFT JOIN LATERAL (
                        SELECT sum(qty) AS total FROM detail
                        WHERE mm_id = m.mm_id AND symbol = m.symbol
                    ) t
                    ORDER BY m.mm_id
                    """)
                    .noLeakCheck()
                    .returns("""
                            mm_id\tsymbol\ttotal
                            1\tAAPL\t300.0
                            2\tGOOG\t300.0
                            3\tMSFT\tnull
                            """);
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

            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    LEFT JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            1\tnull
                            2\tnull
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty\trunning_sum
                            1\t30.0\t30.0
                            1\t20.0\t50.0
                            2\t50.0\t50.0
                            2\t40.0\t90.0
                            3\tnull\tnull
                            """);
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
            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            null\t20.0
                            1\t10.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    LEFT JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            null\t20.0
                            1\t10.0
                            2\tnull
                            """);
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

            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    LATERAL (SELECT qty FROM trades) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty
                            1\t10.0
                            1\t20.0
                            2\t10.0
                            2\t20.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id ORDER BY qty LIMIT o.n) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            1\t10.0
                            1\t20.0
                            2\t40.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id ORDER BY qty LIMIT o.n, 5) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            1\t20.0
                            1\t30.0
                            2\t40.0
                            2\t50.0
                            """);
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

            assertQuery("""
                    SELECT t1.id, sub1.*
                    FROM t1
                    JOIN LATERAL (
                        SELECT t2.id AS id1, t2.t1_id, t2.ts AS ts1,
                               sub2.id AS id2, sub2.t2_id, sub2.ts AS ts2
                        FROM t2
                        JOIN LATERAL (SELECT * FROM t3 WHERE t3.t2_id = t2.id) sub2 ON 1 = 1
                        WHERE t2.t1_id = t1.id
                    ) sub1
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            id\tid1\tt1_id\tts1\tid2\tt2_id\tts2
                            1\t1\t1\t2024-01-01T00:30:00.000000Z\t1\t1\t2024-01-01T01:00:00.000000Z
                            """);
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
            assertQuery("""
                    SELECT o.id, t.ts, t.total
                    FROM orders o
                    JOIN LATERAL (
                        SELECT ts, sum(qty) AS total
                        FROM trades
                        WHERE order_id = o.id
                        SAMPLE BY 30m
                    ) t
                    ORDER BY o.id, t.ts
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tts\ttotal
                            1\t2024-01-01T00:00:00.000000Z\t10.0
                            1\t2024-01-01T00:30:00.000000Z\t20.0
                            1\t2024-01-01T01:00:00.000000Z\t30.0
                            2\t2024-01-01T01:00:00.000000Z\t40.0
                            2\t2024-01-01T01:30:00.000000Z\t50.0
                            """);
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

            assertQuery("""
                    SELECT o.id, t.category, t.ts, t.total
                    FROM orders o
                    JOIN LATERAL (
                        SELECT category, ts, sum(qty) AS total
                        FROM trades
                        WHERE order_id = o.id
                        SAMPLE BY 30m FILL(PREV)
                    ) t
                    ORDER BY o.id, t.category, t.ts
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
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
                            """);
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
            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades_a WHERE order_id = o.id
                        UNION
                        SELECT qty FROM trades_b WHERE order_id = o.id
                    ) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty
                            1\t10.0
                            1\t30.0
                            2\t20.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.max_qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT max(qty) AS max_qty
                        FROM trades
                        WHERE order_id = o.id
                    ) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tmax_qty
                            1\t20.0
                            2\t30.0
                            """);
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

            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades WHERE order_id = o.id
                    ) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            1\t10.0
                            2\t20.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.cat, t.total
                    FROM orders o
                    JOIN LATERAL (
                        SELECT o.category AS cat, sum(qty) AS total
                        FROM trades
                        WHERE order_id = o.id
                        GROUP BY o.category
                    ) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id	cat	total
                            1	X	30.0
                            2	Y	30.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbuy\tsell
                            1\t40.0\t20.0
                            2\tnull\t40.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbuy\tsell
                            1\t10.0\t20.0
                            2\t30.0\tnull
                            3\tnull\tnull
                            """);
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

            // a SAMPLE BY body is keyed, so order 3 produces no row at all and the
            // LEFT JOIN null-extends every column rather than compensating cnt to 0
            assertQuery("""
                    SELECT o.id, t.ts, t.cnt, t.total
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT ts, count(*) AS cnt, sum(qty) AS total
                        FROM trades
                        WHERE order_id = o.id
                        SAMPLE BY 30m
                    ) t
                    ORDER BY o.id, t.ts
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tts\tcnt\ttotal
                            1\t2024-01-01T00:00:00.000000Z\t1\t10.0
                            1\t2024-01-01T00:30:00.000000Z\t1\t20.0
                            2\t2024-01-01T01:00:00.000000Z\t1\t30.0
                            3\t\tnull\tnull
                            """);
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
            assertQuery("""
                    SELECT o.id, t.ts, t.total
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT ts, sum(qty) AS total
                        FROM trades
                        WHERE order_id = o.id
                        SAMPLE BY 30m
                    ) t
                    ORDER BY o.id, t.ts
                    """)
                    .noLeakCheck()
                    .returns("""
                            id	ts	total
                            1	2024-01-01T00:00:00.000000Z	10.0
                            1	2024-01-01T00:30:00.000000Z	20.0
                            2	2024-01-01T01:00:00.000000Z	30.0
                            3		null
                            """);
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

            assertQuery("""
                    SELECT o.id, t.ts, t.total
                    FROM orders o
                    JOIN LATERAL (
                        SELECT ts, sum(qty) AS total
                        FROM trades
                        WHERE order_id = o.id
                        SAMPLE BY 30m ALIGN TO FIRST OBSERVATION
                    ) t
                    ORDER BY o.id, t.ts
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id	ts	total
                            1	2024-01-01T00:05:00.000000Z	10.0
                            1	2024-01-01T00:35:00.000000Z	20.0
                            2	2024-01-01T01:05:00.000000Z	30.0
                            2	2024-01-01T01:35:00.000000Z	40.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.ts, t.total
                    FROM orders o
                    JOIN LATERAL (
                        SELECT ts, sum(qty) AS total
                        FROM trades
                        WHERE order_id = o.id
                        SAMPLE BY 30m FILL(LINEAR)
                    ) t
                    ORDER BY o.id, t.ts
                    """)
                    .noLeakCheck()
                    .returns("""
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
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tts\ttotal
                            1\t2024-01-01T00:00:00.000000Z\t10.0
                            1\t2024-01-01T00:30:00.000000Z\t20.0
                            2\t2024-01-01T01:00:00.000000Z\t40.0
                            2\t2024-01-01T01:30:00.000000Z\t50.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades
                        WHERE order_id = o.id
                        ORDER BY abs(qty - 25) DESC
                    ) t
                    ORDER BY o.id, t.qty DESC
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            1\t30.0
                            1\t20.0
                            1\t10.0
                            2\t50.0
                            2\t40.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            mm_id\tsymbol\tbuy\tsell
                            1\tAAPL\t100.0\t50.0
                            2\tGOOG\t200.0\t300.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcategory\ttotal
                            1\tB\t30.0
                            1\tA\t30.0
                            2\tZ\t70.0
                            2\tY\t60.0
                            """);

            // implicit aggregation
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcategory\ttotal
                            1\tB\t30.0
                            1\tA\t30.0
                            2\tZ\t70.0
                            2\tY\t60.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.category
                    FROM orders o
                    JOIN LATERAL (
                        SELECT DISTINCT category FROM trades
                        WHERE order_id = o.id
                        ORDER BY category
                        LIMIT 2
                    ) t
                    ORDER BY o.id, t.category
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcategory
                            1\tA
                            1\tB
                            2\tX
                            2\tY
                            """);
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

            assertQuery("""
                    SELECT o.id, t.total
                    FROM orders o
                    JOIN LATERAL (
                        WITH matched AS (
                            SELECT qty FROM trades WHERE order_id = o.id
                        )
                        SELECT sum(qty) AS total FROM matched
                    ) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ttotal
                            1\t30.0
                            2\t30.0
                            """);
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

            // the body is a scalar aggregate, so it yields one row per outer row
            assertQuery("""
                    SELECT o.id, t.cnt
                    FROM orders o
                    JOIN LATERAL (SELECT count(*) AS cnt FROM trades WHERE order_id = o.id) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcnt
                            1\t2
                            2\t1
                            3\t0
                            """);
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

            assertQuery("""
                    SELECT o.id, t.cnt
                    FROM orders o
                    LEFT JOIN LATERAL (SELECT count(*) AS cnt FROM trades WHERE order_id = o.id) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcnt
                            1\t2
                            2\t0
                            3\t0
                            """);
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

            assertQuery("""
                    SELECT o.id, t.total
                    FROM orders o
                    LEFT JOIN LATERAL (SELECT sum(qty) AS total FROM trades WHERE order_id = o.id) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ttotal
                            1\t30.0
                            2\tnull
                            3\tnull
                            """);
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
            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty, ts AS trade_ts FROM trades
                        WHERE order_id = o.id AND ts > o.start_ts
                    ) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            1\t20.0
                            1\t30.0
                            2\t50.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades WHERE group_id = o.id / 10
                    ) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty
                            10\t100.0
                            10\t300.0
                            20\t200.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.val
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t1.val
                        FROM t1
                        JOIN t2 ON t2.t1_id = t1.id AND t2.order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tval
                            1\tX
                            2\tY
                            """);
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
            assertQuery("""
                    SELECT o.id, t.total
                    FROM orders o
                    JOIN LATERAL (
                        SELECT sum(qty) AS total FROM trades
                        WHERE order_id = o.id AND qty > o.min_qty
                    ) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ttotal
                            1\t50.0
                            2\t70.0
                            """);
        });
    }

    // T38: OR predicate with correlated ref — no equality predicates, simple scan
    @Test
    public void testT38OrCorrelatedPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQuery("""
                    SELECT o.id, o.customer, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades
                        WHERE order_id = o.id OR (qty > 100.0 AND order_id = o.id)
                    ) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcustomer\tqty
                            1\tAlice\t10.0
                            1\tAlice\t20.0
                            2\tBob\t30.0
                            3\tCharlie\t40.0
                            3\tCharlie\t50.0
                            """);
        });
    }

    // T39: Correlated ref only in SELECT expression — Delim needed
    @Test
    public void testT39CorrelatedRefInSelect() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQuery("""
                    SELECT o.id, t.qty_plus_id
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty + o.id AS qty_plus_id FROM trades
                        WHERE order_id = o.id
                    ) t
                    ORDER BY o.id, t.qty_plus_id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty_plus_id
                            1\t11.0
                            1\t21.0
                            2\t32.0
                            3\t43.0
                            3\t53.0
                            """);
        });
    }

    // T40: Correlated ref in SELECT with aggregate — Delim needed
    @Test
    public void testT40CorrelatedRefInSelectWithAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQuery("""
                    SELECT o.id, t.total_plus_id
                    FROM orders o
                    JOIN LATERAL (
                        SELECT sum(qty) + o.id AS total_plus_id FROM trades
                        WHERE order_id = o.id
                    ) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ttotal_plus_id
                            1\t31.0
                            2\t32.0
                            3\t93.0
                            """);
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
            assertQuery("""
                    SELECT s.id, r.value
                    FROM sensors s
                    JOIN LATERAL (
                        SELECT value FROM readings
                        WHERE sensor_id = s.id AND value > s.threshold
                    ) r
                    ORDER BY s.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tvalue
                            1\t60.0
                            2\t35.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tresult
                            1\t10\t111
                            1\t20\t121
                            2\t10\t112
                            2\t20\t122
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tname\tpriority
                            1\tAlice\t5
                            1\tBob\t3
                            2\tCharlie\t7
                            """);
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

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tname\ttotal
                            1\tGadget\t200.0
                            1\tWidget\t250.0
                            2\tDoohickey\t300.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tresult
                            10\t1\t11
                            10\t2\t12
                            20\t1\t21
                            20\t2\t22
                            """);
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
            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades_a WHERE order_id = o.id
                        EXCEPT
                        SELECT qty FROM trades_b WHERE order_id = o.id
                    ) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty
                            1\t20.0
                            2\t40.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades_a WHERE order_id = o.id
                        INTERSECT ALL
                        SELECT qty FROM trades_b WHERE order_id = o.id
                    ) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty
                            1\t10.0
                            1\t10.0
                            """);
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

            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades_a WHERE order_id = o.id
                        EXCEPT ALL
                        SELECT qty FROM trades_b WHERE order_id = o.id
                    ) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty
                            1\t20.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT qty FROM trades_a WHERE order_id = o.id
                        EXCEPT
                        SELECT qty FROM trades_b WHERE order_id = o.id
                    ) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            1\t20.0
                            2\tnull
                            3\tnull
                            """);
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
            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades_a WHERE order_id = o.id
                        UNION
                        SELECT qty FROM trades_b WHERE order_id = o.id
                    ) t
                    ORDER BY o.id, t.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty
                            1\t10.0
                            2\t10.0
                            3\t10.0
                            """);
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

            assertQuery("""
                    SELECT t1.a, x.result
                    FROM t1
                    CROSS JOIN LATERAL (
                        SELECT val + t1.b AS result
                        FROM (
                            SELECT val FROM t2 WHERE t2.t1_id = t1.a
                        )
                    ) x
                    ORDER BY t1.a, x.result
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tresult
                            10\t101
                            10\t102
                            20\t203
                            """);
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
            assertQuery("""
                    SELECT t1.a, x.val
                    FROM t1
                    CROSS JOIN LATERAL (
                        SELECT val FROM (
                            SELECT val FROM t2 WHERE t2.t1_id = t1.a
                        )
                        WHERE val > t1.threshold
                    ) x
                    ORDER BY t1.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tval
                            1\t10
                            2\t30
                            """);
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

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tresult
                            1\t110
                            2\t230
                            """);

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            a	result1	result
                            1	111	110
                            2	231	230
                            """);


            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            a	column
                            1	1
                            2	1
                            """);

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            a	column
                            2	231
                            """);

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            a	b
                            1	100
                            1	100
                            2	200
                            """);
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
            assertQuery("""
                    SELECT t1.a, x.result
                    FROM t1
                    CROSS JOIN LATERAL (
                        SELECT total * t1.multiplier AS result
                        FROM (
                            SELECT SUM(val) AS total FROM t2 WHERE t2.t1_id = t1.a
                        )
                    ) x
                    ORDER BY t1.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tresult
                            1\t100
                            2\t400
                            """);
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
            assertQuery("""
                    SELECT o.id, t.price
                    FROM (SELECT * FROM orders WHERE status = 'active') o
                    CROSS JOIN LATERAL (
                        SELECT price FROM items
                        WHERE order_id = o.id AND price > o.total
                    ) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tprice
                            1\t60.0
                            3\t250.0
                            """);
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

            assertQuery("""
                    SELECT *
                    FROM t1
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt FROM t2 WHERE t2.t1_id = t1.a
                    ) t ON true
                    ORDER BY t1.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a	ts	cnt
                            1	2024-01-01T00:00:00.000000Z	2
                            2	2024-01-01T01:00:00.000000Z	1
                            3	2024-01-01T02:00:00.000000Z	0
                            """);
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

            assertQuery("""
                    SELECT *
                    FROM t1
                    LEFT JOIN LATERAL (
                        SELECT ts, count(*) AS cnt, sum(val) AS total
                        FROM t2
                        WHERE t2.t1_id = t1.a
                        SAMPLE BY 30m
                    ) t ON true
                    ORDER BY t1.a, t.ts
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tts\tts1\tcnt\ttotal
                            1\t2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:00.000000Z\t1\t10
                            1\t2024-01-01T00:00:00.000000Z\t2024-01-01T00:30:00.000000Z\t1\t20
                            2\t2024-01-01T01:00:00.000000Z\t2024-01-01T01:00:00.000000Z\t1\t30
                            3\t2024-01-01T02:00:00.000000Z\t\tnull\tnull
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.name, sub.tag
                    FROM orders o
                    JOIN LATERAL (
                        SELECT i.name, t.tag
                        FROM items i
                        LEFT JOIN tags t ON t.item_id = i.id AND t.order_id = o.id
                        WHERE i.order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tname\ttag
                            1\tWidget\tpremium
                            2\tGadget\t
                            """);
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
            assertQuery("""
                    SELECT t1.a, sub.val
                    FROM t1
                    JOIN LATERAL (
                        SELECT val FROM t2
                        WHERE t2.t1_id = t1.a
                        ORDER BY abs(val - 5)
                        LIMIT 2
                    ) sub
                    ORDER BY t1.a, sub.val
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tval
                            1\t3
                            1\t7
                            2\t4
                            2\t8
                            """);
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
            assertQuery("""
                    SELECT t1.a, sub.val
                    FROM t1
                    LEFT JOIN LATERAL (
                        SELECT val FROM t2
                        WHERE t2.t1_id = t1.a
                        ORDER BY val DESC
                        LIMIT 2
                    ) sub
                    ORDER BY t1.a, sub.val DESC
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tval
                            1\t30
                            1\t20
                            2\t40
                            3\tnull
                            """);
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
            assertQuery("""
                    SELECT t1.a, sub.val
                    FROM t1
                    JOIN LATERAL (
                        SELECT val FROM t2
                        WHERE t2.t1_id = t1.a
                        ORDER BY val DESC
                        LIMIT 1, 2
                    ) sub
                    ORDER BY t1.a, sub.val DESC
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tval
                            1\t30
                            2\t70
                            """);
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

            assertQuery("""
                    SELECT o.id, t.trade_cnt, f.fill_cnt
                    FROM orders o
                    JOIN LATERAL (
                        SELECT count(*) AS trade_cnt FROM trades WHERE order_id = o.id
                    ) t
                    JOIN LATERAL (
                        SELECT count(*) AS fill_cnt FROM fills WHERE order_id = o.id
                    ) f
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ttrade_cnt\tfill_cnt
                            1\t2\t1
                            2\t1\t2
                            """);
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

            assertQuery("""
                    SELECT c.id, p.id, s.total_qty
                    FROM customers c
                    CROSS JOIN products p
                    LEFT JOIN LATERAL (
                        SELECT sum(qty)::INT AS total_qty
                        FROM sales
                        WHERE customer_id = c.id AND product_id = p.id
                    ) s
                    ORDER BY c.id, p.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tid1\ttotal_qty
                            1\t10\t5
                            1\t20\t3
                            2\t10\t7
                            2\t20\tnull
                            """);
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
            assertQuery("""
                    SELECT t1.id, sub.val, sub.rn
                    FROM t1
                    JOIN LATERAL (
                        SELECT val, row_number() OVER (ORDER BY ts) AS rn
                        FROM t2
                        WHERE t2.t1_id = t1.id
                    ) sub ON sub.rn <= t1.n
                    ORDER BY t1.id, sub.rn
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tval\trn
                            1\t10\t1
                            1\t20\t2
                            2\t40\t1
                            2\t50\t2
                            2\t60\t3
                            """);
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
            assertQuery("""
                    SELECT t1.id, sub.val, sub.rn
                    FROM t1
                    LEFT JOIN LATERAL (
                        SELECT val, row_number() OVER (ORDER BY ts) AS rn
                        FROM t2
                        WHERE t2.t1_id = t1.id
                    ) sub ON sub.rn <= t1.n
                    ORDER BY t1.id, sub.rn
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tval\trn
                            1\t10\t1
                            1\t20\t2
                            2\t40\t1
                            3\tnull\tnull
                            """);
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
            assertQuery("""
                    SELECT t1.id, sub.val, sub.rn
                    FROM t1
                    JOIN LATERAL (
                        SELECT val, cat, row_number() OVER (ORDER BY ts) AS rn
                        FROM t2
                        WHERE t2.t1_id = t1.id
                    ) sub ON sub.cat = t1.cat AND sub.rn <= t1.n
                    ORDER BY t1.id, sub.rn
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tval\trn
                            1\t10\t1
                            1\t20\t2
                            2\t40\t1
                            2\t50\t2
                            2\t60\t3
                            """);
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

            assertQuery("""
                    SELECT t1.id, sub.val
                    FROM t1
                    JOIN LATERAL (
                        SELECT val, cat
                        FROM t2
                        WHERE t2.t1_id = t1.id
                    ) sub ON sub.cat = t1.cat
                    ORDER BY t1.id, sub.val
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\t10
                            1\t20
                            2\t30
                            """);
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

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            mm_id\tsymbol\tobligation_id\tnon_compliance_reason\tavg_price_buy\tavg_price_sell\tqty_buy\tqty_sell\tspread
                            mm1\tAAPL\t1\tNone\t100.0\t102.0\t300.0\t300.0\t0.020000000000000018
                            mm2\tAAPL\t2\tSellQuantity\tnull\tnull\t100.0\t0.0\tnull
                            mm3\tMSFT\t3\tSpread\t100.0\t125.0\t200.0\t200.0\t0.25
                            mm4\tMSFT\t4\tNone\t50.0\t51.0\t200.0\t200.0\t1.0
                            mm5\tGOOG\t5\tQuantity\tnull\tnull\t30.0\t30.0\tnull
                            mm6\tGOOG\t6\tBuyQuantity\tnull\tnull\t0.0\t200.0\tnull
                            """);
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

            assertQuery("SELECT * FROM mm_compliance ORDER BY mm_id, symbol")
                    .noLeakCheck()
                    .returns("""
                            mm_id\tsymbol\tobligation_id\tnon_compliance_reason\tavg_price_buy\tavg_price_sell\tqty_buy\tqty_sell\tspread
                            mm1\tAAPL\t1\tNone\t100.0\t102.0\t200.0\t200.0\t0.020000000000000018
                            mm2\tAAPL\t2\tSellQuantity\tnull\tnull\t100.0\t0.0\tnull
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ttotal_qty\tfee
                            1\t30.0\t0.5
                            1\t30.0\t1.0
                            2\t30.0\t0.3
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty\trank\tbonus
                            1\t30.0\t1\t100.0
                            2\t50.0\t1\t100.0
                            """);
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

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            a\tb\tc\td
                            1\t10\t100\t1000
                            """);
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

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty
                            1\t10
                            1\t20
                            2\t30
                            2\t40
                            """);
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
            assertQuery("""
                    SELECT o.id, t.val
                    FROM orders o
                    JOIN LATERAL (
                        SELECT sum(qty) AS val FROM trades WHERE order_id = o.id
                        UNION ALL
                        SELECT qty AS val FROM trades WHERE order_id = o.id
                    ) t
                    ORDER BY o.id, t.val
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tval
                            1\t10.0
                            1\t20.0
                            1\t30.0
                            2\t30.0
                            2\t30.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tc
                            1\t10\t100
                            2\t20\t200
                            """);
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
            assertQuery("""
                    SELECT o.id, a.total_qty, b.rate
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT sum(qty) AS total_qty FROM trades WHERE order_id = o.id
                    ) a
                    LEFT JOIN LATERAL (
                        SELECT rate FROM discounts WHERE min_qty <= a.total_qty
                    ) b
                    ORDER BY o.id, b.rate
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ttotal_qty\trate
                            1\t100.0\t0.1
                            1\t100.0\t0.2
                            2\t20.0\tnull
                            3\tnull\tnull
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.item_cnt, sub.total_paid
                    FROM orders o
                    JOIN LATERAL (
                        SELECT ic.item_cnt, ps.total_paid
                        FROM (SELECT count(*)::INT AS item_cnt FROM items WHERE order_id = o.id) ic
                        CROSS JOIN (SELECT sum(amount) AS total_paid FROM payments WHERE order_id = o.id) ps
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\titem_cnt\ttotal_paid
                            1\t2\t50.0
                            2\t1\t50.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcategory\ttotal
                            2\tX\t25.0
                            2\tY\t30.0
                            """);
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
            assertQuery("""
                    SELECT t1.id, sub.val
                    FROM t1
                    JOIN LATERAL (
                        SELECT val FROM t2 WHERE t2.t1_id = t1.id ORDER BY val LIMIT abs(2)
                    ) sub
                    ORDER BY t1.id, sub.val
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\t10
                            1\t20
                            """);
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

            assertQuery("""
                    SELECT t1.id, sub.val, sub.doubled_running
                    FROM t1
                    JOIN LATERAL (
                        SELECT val, (sum(val) OVER (ORDER BY ts)) * 2 AS doubled_running
                        FROM t2
                        WHERE t2.t1_id = t1.id
                    ) sub
                    ORDER BY t1.id, sub.val
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id	val	doubled_running
                            1	10	20.0
                            1	20	60.0
                            2	30	60.0
                            2	40	140.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tval\trn
                            1\t10\t1
                            1\t20\t2
                            2\t30\t1
                            2\t40\t2
                            """);
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
            assertQuery("""
                    SELECT *
                    FROM t1
                    JOIN LATERAL (
                        SELECT val, ts FROM t2 WHERE t2.t1_a = t1.a
                    ) sub
                    ORDER BY t1.a, sub.val
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tts\tval\tts1
                            1\t2024-01-01T00:00:00.000000Z\t10\t2024-01-01T00:10:00.000000Z
                            1\t2024-01-01T00:00:00.000000Z\t20\t2024-01-01T00:20:00.000000Z
                            2\t2024-01-01T01:00:00.000000Z\t30\t2024-01-01T01:10:00.000000Z
                            """);
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
            assertQuery("""
                    SELECT t1.id, sub.val
                    FROM t1
                    JOIN LATERAL (
                        SELECT val FROM t2 WHERE t2.t1_id = t1.id
                        UNION ALL
                        SELECT val FROM t3 WHERE t3.threshold < t1.id
                    ) sub
                    ORDER BY t1.id, sub.val
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tval
                            1\t10
                            1\t100
                            2\t20
                            2\t100
                            2\t200
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcategory\tqty
                            1\tX\t15.0
                            1\tY\t5.0
                            2\tX\t20.0
                            2\tY\t30.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.category, t.qty
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT category, qty FROM trades
                        WHERE order_id = o.id
                        LATEST ON ts PARTITION BY category
                    ) t
                    ORDER BY o.id, t.category
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcategory\tqty
                            1\tA\t20.0
                            1\tB\t30.0
                            2\tA\t40.0
                            3\t\tnull
                            """);
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
            assertQuery("""
                    SELECT m.mm_id, m.symbol, q.side, q.price
                    FROM markets m
                    JOIN LATERAL (
                        SELECT side, price FROM quotes
                        WHERE mm_id = m.mm_id AND symbol = m.symbol
                        LATEST ON ts PARTITION BY side
                    ) q
                    ORDER BY m.mm_id, q.side
                    """)
                    .noLeakCheck()
                    .returns("""
                            mm_id\tsymbol\tside\tprice
                            mm1\tAAPL\tASK\t101.0
                            mm1\tAAPL\tBID\t100.0
                            mm2\tAAPL\tASK\t103.0
                            mm2\tAAPL\tBID\t98.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.category, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT category, qty FROM trades
                        WHERE ts > o.start_ts
                        LATEST ON ts PARTITION BY category
                    ) t
                    ORDER BY o.id, t.category
                    """)
                    .noLeakCheck()
                    .returns("""
                            id	category	qty
                            1	A	40.0
                            1	B	50.0
                            2	A	40.0
                            2	B	50.0
                            """);

            assertQuery("""
                    SELECT o.id, t.*
                    FROM orders o
                    JOIN LATERAL (
                        SELECT category, qty
                        FROM (
                            SELECT category, qty FROM trades
                            WHERE ts > o.start_ts
                            LATEST ON ts PARTITION BY category
                        ) latest_trade
                    ) t
                    ORDER BY o.id, t.category
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcategory\tqty
                            1\tA\t40.0
                            1\tB\t50.0
                            2\tA\t40.0
                            2\tB\t50.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades
                        LATEST ON ts PARTITION BY id
                    ) t
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            1\t30.0
                            2\t30.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.category, t.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT category, qty FROM trades
                        WHERE order_id = o.id AND ts > o.min_ts
                        LATEST ON ts PARTITION BY category
                    ) t
                    ORDER BY o.id, t.category
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcategory\tqty
                            1\tA\t20.0
                            1\tB\t30.0
                            2\tA\t50.0
                            2\tB\t60.0
                            """);
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
            assertQuery("""
                    SELECT o.id, t.category, t.qty
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT category, qty FROM trades
                        WHERE ts > o.start_ts
                        LATEST ON ts PARTITION BY category
                    ) t
                    ORDER BY o.id, t.category
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcategory\tqty
                            1\tA\t20.0
                            1\tB\t30.0
                            2\t\tnull
                            """);
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
            assertQuery("""
                    SELECT t1.id, sub.val
                    FROM t1
                    JOIN LATERAL (SELECT val FROM t2 WHERE t2.t1_id = t1.id) sub
                    ORDER BY t1.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            """);

            // Empty outer with LEFT → 0 rows (LEFT doesn't create rows from nothing)
            assertQuery("""
                    SELECT t1.id, sub.val
                    FROM t1
                    LEFT JOIN LATERAL (SELECT val FROM t2 WHERE t2.t1_id = t1.id) sub
                    ORDER BY t1.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            """);
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
            assertQuery("""
                    SELECT t1.id, sub.val
                    FROM t1
                    JOIN LATERAL (
                        SELECT val FROM t2
                        WHERE t2.t1_id = t1.id
                        ORDER BY val * t1.sort_dir
                    ) sub
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id	val
                            1	20
                            1	10
                            2	30
                            2	40
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty\trn_ts\trn_qty
                            1\t10.0\t1\t3
                            1\t30.0\t2\t1
                            1\t20.0\t3\t2
                            2\t50.0\t1\t1
                            2\t40.0\t2\t2
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty\tprice\trunning_qty\tprev_price\tprice_rank\tavg_qty
                            1\t10.0\t100.0\t10.0\tnull\t3\t10.0
                            1\t20.0\t200.0\t30.0\t100.0\t1\t15.0
                            1\t30.0\t150.0\t60.0\t200.0\t2\t20.0
                            2\t40.0\t300.0\t40.0\tnull\t2\t40.0
                            2\t50.0\t250.0\t90.0\t300.0\t3\t45.0
                            2\t60.0\t350.0\t150.0\t250.0\t1\t50.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tside\tqty\trn\tside_total
                            1\tBUY\t10.0\t1\t10.0
                            1\tBUY\t30.0\t2\t40.0
                            1\tSELL\t20.0\t1\t20.0
                            2\tBUY\t40.0\t1\t40.0
                            2\tBUY\t50.0\t2\t90.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tval\tlabel
                            1\t10.0\ttrade
                            1\t5.0\trefund
                            2\t20.0\ttrade
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttotal_qty\ttag
                            1\t30.0\tVIP
                            2\t30.0\tSTD
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.name, sub.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT p.name, t.qty
                        FROM trades t
                        JOIN products p ON p.id = t.product_id
                        WHERE t.order_id = o.id
                    ) sub
                    ORDER BY o.id, sub.name
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tname\tqty
                            1\tApple\t10.0
                            1\tBanana\t20.0
                            2\tApple\t30.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.val
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty AS val FROM trades WHERE order_id = o.id
                        UNION ALL
                        SELECT 0 AS val FROM long_sequence(1)
                    ) sub
                    ORDER BY o.id, sub.val
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tval
                            1\t0.0
                            1\t10.0
                            2\t0.0
                            2\t20.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty, sub.price
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, p.price
                        FROM trades t
                        ASOF JOIN prices p
                        WHERE t.order_id = o.id
                    ) sub
                    ORDER BY o.id, sub.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tprice
                            1\t10.0\t100.0
                            1\t20.0\t101.0
                            2\t30.0\t102.0
                            """);

            assertQuery("""
                    SELECT o.id, sub.qty, sub.price, sub.rn
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, p.price, row_number() OVER (ORDER BY p.ts DESC) AS rn
                        FROM trades t
                        ASOF JOIN prices p
                        WHERE t.order_id = o.id
                    ) sub
                    ORDER BY o.id, sub.qty
                    """)
                    .noLeakCheck()
                    .returns("""
                            id	qty	price	rn
                            1	10.0	100.0	2
                            1	20.0	101.0	1
                            2	30.0	102.0	1
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty, sub.max_qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, l.max_qty
                        FROM trades t
                        JOIN limits l ON l.category = t.category
                        WHERE t.category = o.category
                    ) sub
                    ORDER BY o.id, sub.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tmax_qty
                            1\t10.0\t15.0
                            1\t20.0\t15.0
                            2\t30.0\t50.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.adjusted_qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty * f.factor AS adjusted_qty
                        FROM trades t
                        JOIN factors f ON f.trade_order_id = t.order_id
                        WHERE t.order_id = o.id AND t.qty > o.min_qty
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tadjusted_qty
                            1\t30.0
                            2\t60.0
                            """);
        });
    }

    @Test
    public void testT87bCommaLateral() throws Exception {
        assertQuery("""
                SELECT ss1.x, ss2.y, ss3.z FROM
                  (SELECT 1 AS x) ss1
                  LEFT JOIN (SELECT 2 AS y) ss2 ON (true),
                  LATERAL (SELECT ss2.y AS z FROM long_sequence(1) LIMIT 1) ss3
                """)
                .noRandomAccess()
                .expectSize()
                .returns("""
                        x\ty\tz
                        1\t2\t2
                        """);
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
            assertQuery("""
                    SELECT t1.a, sub.val
                    FROM t1
                    JOIN LATERAL (
                        SELECT val FROM t2 WHERE t1_a = a
                    ) sub
                    ORDER BY t1.a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tval
                            1\t10
                            2\t20
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcategory\ttotal
                            1\tX\t30.0
                            1\tX\t5.0
                            2\tY\t30.0
                            2\tY\t8.0
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcategory\ttotal
                            1\tA\t10.0
                            1\tB\t20.0
                            2\tA\t40.0
                            2\tB\t50.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.total
                    FROM (SELECT id, ts FROM orders WHERE status = 'active') o
                    JOIN LATERAL (
                        SELECT sum(qty) AS total FROM trades WHERE order_id = o.id
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ttotal
                            1\t30.0
                            3\t30.0
                            """);
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
            assertQuery("""
                    SELECT *
                    FROM t1
                    JOIN LATERAL (
                        SELECT val, ts FROM t2
                        WHERE t2.t1_id = t1.id AND t2.val > t1.threshold
                    ) sub
                    ORDER BY t1.id, sub.val
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tthreshold\tts\tval\tts1
                            1\t15\t2024-01-01T00:00:00.000000Z\t20\t2024-01-01T00:20:00.000000Z
                            2\t25\t2024-01-01T01:00:00.000000Z\t30\t2024-01-01T01:10:00.000000Z
                            2\t25\t2024-01-01T01:00:00.000000Z\t40\t2024-01-01T01:20:00.000000Z
                            """);
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

            assertQuery("""
                    SELECT o.id, sub.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades
                        WHERE order_id = o.id
                          AND order_id IN (SELECT order_id FROM valid_orders)
                    ) sub
                    ORDER BY o.id
                    """)
                    .fails(126, "cannot compare LONG with type CURSOR");
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
            assertQuery("""
                    SELECT o.id, sub.category, sub.total
                    FROM orders o
                    JOIN LATERAL (
                        SELECT DISTINCT category, sum(qty) AS total
                        FROM trades
                        WHERE order_id = o.id
                        GROUP BY category
                    ) sub
                    ORDER BY o.id, sub.category
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcategory\ttotal
                            1\tA\t20.0
                            1\tB\t20.0
                            2\tA\t30.0
                            """);
        });
    }

    // T95: LEFT LATERAL count — outer table has column with same name as count alias
    // Scalar-count identity must NOT wrap t1.cnt with coalesce
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
            assertQuery("""
                    SELECT t1.cnt, sub.cnt
                    FROM t1
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt FROM t2 WHERE t2.t1_id = t1.id
                    ) sub ON true
                    ORDER BY t1.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            cnt\tcnt1
                            100\t2
                            200\t1
                            300\t0
                            """);
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
            assertQuery("""
                    SELECT *
                    FROM t1
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt FROM t2 WHERE t2.t1_id = t1.id
                    ) sub ON true
                    ORDER BY t1.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcnt\tts\tcnt1
                            1\t100\t2024-01-01T00:00:00.000000Z\t2
                            2\t200\t2024-01-01T01:00:00.000000Z\t1
                            3\t300\t2024-01-01T02:00:00.000000Z\t0
                            """);
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

            // GROUP BY can remove the aggregate row. Both counts must remain NULL for an unmatched group.
            assertQuery("""
                    SELECT *
                    FROM t1
                    LEFT JOIN LATERAL (
                        SELECT category, count(*) AS cnt_all, count(category) AS cnt_cat
                        FROM t2
                        WHERE t2.t1_id = t1.id
                        GROUP BY category
                    ) sub ON true
                    ORDER BY t1.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tts\tcategory\tcnt_all\tcnt_cat
                            1\t2024-01-01T00:00:00.000000Z\tA\t2\t2
                            2\t2024-01-01T01:00:00.000000Z\tB\t1\t1
                            3\t2024-01-01T02:00:00.000000Z\t\tnull\tnull
                            """);
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
            assertQuery("""
                    SELECT t1.id, sub.cnt, sub.total
                    FROM t1
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt, sum(val) AS total
                        FROM t2
                        WHERE t2.t1_id = t1.id
                    ) sub ON true
                    ORDER BY t1.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcnt\ttotal
                            1\t2\t30
                            2\t1\t30
                            3\t0\tnull
                            """);
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
            assertQuery("""
                    SELECT *
                    FROM t1
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt, sum(val) AS total
                        FROM t2
                        WHERE t2.t1_id = t1.id
                    ) sub ON true
                    ORDER BY t1.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tts\tcnt\ttotal
                            1\t2024-01-01T00:00:00.000000Z\t2\t30
                            2\t2024-01-01T01:00:00.000000Z\t1\t30
                            3\t2024-01-01T02:00:00.000000Z\t0\tnull
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades
                        WHERE order_id = o.id
                        ORDER BY qty
                        LIMIT 0
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty
                    FROM orders o
                    JOIN LATERAL (
                        SELECT qty FROM trades
                        WHERE order_id = o.id AND qty > o.threshold
                    ) sub
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tqty
                            1\t20.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.cnt
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM trades
                        WHERE order_id >= o.id AND order_id < o.id + 1
                    ) sub ON true
                    WHERE o.status = 'ACTIVE'
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcnt
                            1\t2
                            3\t1
                            4\t0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.cnt
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM trades
                        WHERE order_id >= o.id AND order_id < o.id + 1
                    ) sub ON true
                    WHERE status = 'ACTIVE'
                    ORDER BY o.id
                    """)
                    .withPlan("""
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
                            """)
                    .returns("""
                            id\tcnt
                            1\t2
                            3\t0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.cnt
                    FROM orders o
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS cnt
                        FROM trades
                        WHERE order_id >= o.id AND order_id < o.id + 1
                    ) sub ON true
                    WHERE abs(o.id) > 1
                    ORDER BY o.id
                    """)
                    .withPlan("""
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
                            """)
                    .returns("""
                            id\tcnt
                            2\t1
                            3\t1
                            """);
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
            assertQuery("""
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
                    """)
                    .withPlan("""
                            Encode sort
                              keys: [id]
                                VirtualRecord
                                  functions: [id,category,coalesce(cnt,0)]
                                    SelectedRecord
                                        Hash Left Outer Join Light
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
                            """)
                    .returns("""
                            id\tcategory\tcnt
                            1\tX\t2
                            3\tX\t1
                            """);
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
            assertQuery("""
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
                    """)
                    .withPlan("""
                            Encode sort
                              keys: [id]
                                VirtualRecord
                                  functions: [id,t2_val,coalesce(cnt,0)]
                                    SelectedRecord
                                        Hash Left Outer Join Light
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
                            """)
                    .returns("""
                            id\tt2_val\tcnt
                            1\t5\t1
                            3\t15\t1
                            """);
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

            assertQuery("""
                    SELECT w.id, sub.val
                    FROM windows w
                    JOIN LATERAL (
                        SELECT val FROM events
                        WHERE ts >= w.start_ts AND ts < w.end_ts
                    ) sub
                    ORDER BY w.id, sub.val
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tval
                            1\t10
                            1\t20
                            2\t40
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty, sub.adj
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, a.adj
                        FROM trades t
                        JOIN adjustments a ON a.order_id = o.id
                        WHERE t.order_id = o.id
                    ) sub
                    ORDER BY o.id, sub.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tadj
                            1\t10.0\t1.5
                            1\t20.0\t1.5
                            2\t30.0\t2.0
                            """);
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
            assertQuery("""
                    SELECT o.id, sub.qty, sub.adj
                    FROM orders o
                    JOIN LATERAL (
                        SELECT t.qty, a.adj
                        FROM trades t
                        LEFT JOIN adjustments a ON a.order_id = o.id
                        WHERE t.order_id = o.id
                    ) sub
                    ORDER BY o.id, sub.qty
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tadj
                            1\t10.0\t1.5
                            1\t20.0\t1.5
                            2\t30.0\tnull
                            """);
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
            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tqty\tadj\tdisc
                            1\t10.0\t1.5\t0.9
                            2\t30.0\t2.0\t0.8
                            """);
        });
    }

    // Binds $1 to a non-NULL value, under which order 1 returns trade 10 with refund 100, then to NULL,
    // under which the query returns nullRows, then to the value again
    private static ObjList<BindVarTuple> nullValueBindCases(int value, String nullRows) {
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok("non-NULL value", "id\ttid\trid\n1\t10\t100\n", bindVariableService -> bindVariableService.setInt(0, value)));
        cases.add(BindVarTuple.ok("NULL value", "id\ttid\trid\n" + nullRows, bindVariableService -> bindVariableService.setInt(0, Numbers.INT_NULL)));
        cases.add(BindVarTuple.ok("non-NULL value again", "id\ttid\trid\n1\t10\t100\n", bindVariableService -> bindVariableService.setInt(0, value)));
        return cases;
    }

    private void assertPlainScalarAggregateRuntimeLimit(
            CharSequence sql,
            String expectedAtOne,
            String expectedAtZero
    ) throws Exception {
        execute("CREATE TABLE t1 (k INT)");
        execute("INSERT INTO t1 VALUES (1), (2)");
        execute("CREATE TABLE t2 (k INT, v INT)");
        execute("INSERT INTO t2 VALUES (1, 10), (1, 20), (2, 30)");

        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok(
                "limit keeps the plain aggregate row",
                expectedAtOne,
                bindVariableService -> bindVariableService.setLong("lim", 1)
        ));
        cases.add(BindVarTuple.ok(
                "limit drops the plain aggregate row",
                expectedAtZero,
                bindVariableService -> bindVariableService.setLong("lim", 0)
        ));
        cases.add(BindVarTuple.ok(
                "limit keeps the plain aggregate row again",
                expectedAtOne,
                bindVariableService -> bindVariableService.setLong("lim", 1)
        ));

        assertQuery(sql)
                .noLeakCheck()
                .sizeMayVary()
                .assertBinds(cases);
    }

    // The query of testLateralCorrelatedRightJoinSubQueryWhereKeyLaterInnerJoin() with laterJoin in
    // place of its INNER join returns the rows of expected
    private void assertRightJoinSubQueryWhereKeyLaterJoin(String laterJoin, String expected) throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyAndLaterJoinTables();
            assertQuery("SELECT o.id, l.aid, l.sid, l.cid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid, c.id cid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k " + laterJoin + " WHERE a.x = o.x) l ORDER BY 1, 2, 3, 4")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns(expected);
        });
    }

    // The query of testLateralCorrelatedRightJoinSubQueryWhereKeyLaterInnerJoin() with a SPLICE join in
    // place of its INNER join fails at the SPLICE join, as it does when it runs once per outer row, as
    // its tables have no designated timestamp, and the tables still serve that query.
    private void assertRightJoinSubQueryWhereKeyLaterSpliceJoinFails() throws Exception {
        assertMemoryLeak(() -> {
            createNullOuterKeyAndLaterJoinTables();
            final String sql = "SELECT o.id, l.aid, l.sid, l.cid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid, c.id cid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k SPLICE JOIN c WHERE a.x = o.x) l";
            assertQuery(sql)
                    .noLeakCheck()
                    .fails(sql.indexOf("SPLICE"), "left side of time series join has no timestamp");
            assertQuery("SELECT o.id, l.aid, l.sid, l.cid FROM o JOIN LATERAL (SELECT a.id aid, s.id sid, c.id cid FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k JOIN c ON c.sid = s.id WHERE a.x = o.x) l ORDER BY 1, 2, 3, 4")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid\tcid
                            1\t10\t21\t100
                            2\tnull\t22\t101
                            """);
        });
    }

    // Compares the LATERAL query over o, whose body reads o.x as {X} and o.id as {I}, with the body run
    // once per outer row, with the o.x and o.id of that row as constants
    private void assertLateralMatchesPerOuterRow(String body) throws Exception {
        final String select = "SELECT a.id aid, b.id sid FROM ";
        assertSqlCursors(
                "SELECT * FROM ("
                        + "SELECT 1 id, a.id aid, b.id sid FROM " + body.replace("{X}", "1").replace("{I}", "1") + " UNION ALL "
                        + "SELECT 2 id, a.id aid, b.id sid FROM " + body.replace("{X}", "NULL::int").replace("{I}", "2") + " UNION ALL "
                        + "SELECT 3 id, a.id aid, b.id sid FROM " + body.replace("{X}", "3").replace("{I}", "3")
                        + ") ORDER BY 1, 2, 3",
                "SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (" + select + body.replace("{X}", "o.x").replace("{I}", "o.id") + ") l ORDER BY 1, 2, 3"
        );
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

    // Runs the query 60 times, with the filter on a sub-query that rnd_int() makes NULL in about
    // half the executions. Each execution returns 1 10 100, the rows of the value 0, or the refunds
    // that the RIGHT join NULL-extends, the rows of NULL, as NULL >= NULL is true. Both occur.
    private void assertScalarSubQueryFilterOfEitherValue(String filter) throws Exception {
        final String sql = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE " + filter + ") l ORDER BY 1, 2, 3";
        sqlExecutionContext.setRandom(new Rnd(42, 4242));
        int valueCount = 0;
        int nullCount = 0;
        try (RecordCursorFactory factory = select(sql)) {
            for (int i = 0; i < 60; i++) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                }
                if (Chars.equals("id\ttid\trid\n1\t10\t100\n", sink)) {
                    valueCount++;
                } else {
                    TestUtils.assertEquals("execution " + i, "id\ttid\trid\n" + UNMATCHED_REFUNDS, sink);
                    nullCount++;
                }
            }
        }
        Assert.assertTrue(filter, valueCount > 0);
        Assert.assertTrue(filter, nullCount > 0);
    }

    // Trade 10 matches refund 100 only for o.k = 1. Every other refund, and trade 10 for
    // o.k = 2, is unmatched for some outer row.
    private void createCorrelatedRightJoinTables() throws Exception {
        execute("CREATE TABLE orders (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE trades (id INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE refunds (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE xs (k INT, v INT)");
        execute("INSERT INTO orders VALUES (1, 1, 1::timestamp), (2, 2, 2::timestamp)");
        execute("INSERT INTO trades VALUES (10, 1, 1::timestamp)");
        execute("INSERT INTO refunds VALUES (100, 1, 1::timestamp), (101, 2, 2::timestamp)");
        execute("INSERT INTO xs VALUES (1, 100)");
    }

    // The rows of a LATERAL query over the tables of createKeyAndComparisonTables().
    private void assertKeyAndComparison(boolean expectSize, String sql, String expected) throws Exception {
        assertMemoryLeak(() -> {
            createKeyAndComparisonTables();
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize(expectSize)
                    .returns(expected);
        });
    }

    // Per outer row, WHERE a.x = o.x selects a rows 10 and 0 for o.x = 1, row 12 for a NULL o.x, as
    // = matches NULL to NULL, and row 11 for o.x = 3; a.id > o.x then keeps rows 10 and 11.
    // s = (SELECT id, k, x FROM b WHERE x = o.x) holds b rows 20 and 21 for o.x = 1, rows 22 and 23
    // for a NULL o.x and row 24 for o.x = 3. On a.x = s.k, a rows 10 and 0 match b rows 21 and 23,
    // and a row 11 matches b row 24. c rows 100 to 103 match b rows 21, 22, 20 and 23 by sid.
    private void createKeyAndComparisonTables() throws Exception {
        execute("CREATE TABLE o (id INT, x INT)");
        execute("CREATE TABLE a (id INT, x INT)");
        execute("CREATE TABLE b (id INT, k INT, x INT)");
        execute("CREATE TABLE c (id INT, sid INT)");
        execute("INSERT INTO o VALUES (1, 1), (2, NULL), (3, 3)");
        execute("INSERT INTO a VALUES (10, 1), (11, 3), (12, NULL), (0, 1)");
        execute("INSERT INTO b VALUES (20, 5, 1), (21, 1, 1), (22, 7, NULL), (23, 1, NULL), (24, 3, 3)");
        execute("INSERT INTO c VALUES (100, 21), (101, 22), (102, 20), (103, 23), (104, 99)");
    }

    // Timestamped rows of a, c and b: c.sid = a.id matches each a row, and the b rows, with keys 3,
    // 3, 1 and NULL, fall between the a rows in time, so SPLICE and ASOF pair each a row with
    // other b rows.
    private void createSpliceKeyRows() throws Exception {
        execute("INSERT INTO a VALUES (1, 3, 1::timestamp), (2, 1, 3::timestamp), (3, NULL, 6::timestamp)");
        execute("INSERT INTO c VALUES (100, 1, 1::timestamp), (101, 2, 2::timestamp), (102, 3, 3::timestamp)");
        execute("INSERT INTO b VALUES (20, 3, 2::timestamp), (21, 3, 4::timestamp), (22, 1, 5::timestamp), (23, NULL, 7::timestamp)");
    }

    // t holds, for k = 1, rows (p, 5) at 01:00, (p, 0) at 02:00 and (q, 7) at 04:00, for a NULL k row
    // (p, 9) at 05:00, and for k = 2 rows (q, 8) at 05:30 and (q, 1) at 06:00. o rows 1 and 2 share
    // k = 1 with different start times, row 3 has a NULL k.
    private void createLatestOnKeyTables() throws Exception {
        execute("CREATE TABLE o (id INT, k INT, start_ts TIMESTAMP)");
        execute("CREATE TABLE t (s SYMBOL, k INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("""
                INSERT INTO o VALUES
                (1, 1, '2024-01-01T00:00:00.000000Z'),
                (2, 1, '2024-01-01T03:00:00.000000Z'),
                (3, NULL, '2024-01-01T00:00:00.000000Z'),
                (4, 2, '2024-01-01T05:45:00.000000Z')
                """);
        execute("""
                INSERT INTO t VALUES
                ('p', 1, 5, '2024-01-01T01:00:00.000000Z'),
                ('p', 1, 0, '2024-01-01T02:00:00.000000Z'),
                ('q', 1, 7, '2024-01-01T04:00:00.000000Z'),
                ('p', NULL, 9, '2024-01-01T05:00:00.000000Z'),
                ('q', 2, 8, '2024-01-01T05:30:00.000000Z'),
                ('q', 2, 1, '2024-01-01T06:00:00.000000Z')
                """);
    }

    // createNullOuterKeyTables() and c, whose rows 100 and 101 match b rows 21 and 22, the rows that
    // testLateralCorrelatedRightJoinSubQueryWhereKey() returns, by sid.
    private void createNullOuterKeyAndLaterJoinTables() throws Exception {
        createNullOuterKeyTables();
        execute("CREATE TABLE c (id INT, sid INT)");
        execute("INSERT INTO c VALUES (100, 21), (101, 22), (102, 20), (103, 23)");
    }

    // s = (SELECT id, k, x FROM b WHERE x = o.x) holds b rows 20 and 21 for o.x = 1, rows 22 and
    // 23 for a NULL o.x, as = matches NULL to NULL, and no row for o.x = 3. On a.x = s.k, a
    // matches row 21 and row 23.
    private void createNullOuterKeyTables() throws Exception {
        execute("CREATE TABLE o (id INT, x INT)");
        execute("CREATE TABLE a (id INT, x INT)");
        execute("CREATE TABLE b (id INT, k INT, x INT)");
        execute("INSERT INTO o VALUES (1, 1), (2, NULL), (3, 3)");
        execute("INSERT INTO a VALUES (10, 1)");
        execute("INSERT INTO b VALUES (20, 5, 1), (21, 1, 1), (22, 7, NULL), (23, 1, NULL)");
    }

    // o.ts is TIMESTAMP, a.ts and a.y are TIMESTAMP_NS, which equal a TIMESTAMP at the same instant.
    // t holds the ids of a. a2.ts and a2.y are TIMESTAMP. o.ts selects a row 10 and a2 row 12 for
    // o row 1 (1us), a row 11 and a2 row 13 for o row 2 (5us), and no row for the NULL o.ts of o
    // row 3.
    private void createRetypedKeyComparisonTables() throws Exception {
        execute("CREATE TABLE o (id INT, ts TIMESTAMP)");
        execute("CREATE TABLE a (id INT, ts TIMESTAMP_NS, y TIMESTAMP_NS)");
        execute("CREATE TABLE t (id INT)");
        execute("CREATE TABLE a2 (id INT, ts TIMESTAMP, y TIMESTAMP)");
        execute("INSERT INTO o VALUES (1, '2024-01-01T00:00:00.000001Z'), (2, '2024-01-01T00:00:00.000005Z'), (3, NULL)");
        execute("""
                INSERT INTO a VALUES
                (10, '2024-01-01T00:00:00.000001000Z', '2024-01-01T00:00:00.000001500Z'),
                (11, '2024-01-01T00:00:00.000005000Z', '2024-01-01T00:00:00.000007000Z')
                """);
        execute("INSERT INTO t VALUES (10), (11)");
        execute("""
                INSERT INTO a2 VALUES
                (12, '2024-01-01T00:00:00.000001Z', '2024-01-01T00:00:00.000002Z'),
                (13, '2024-01-01T00:00:00.000005Z', '2024-01-01T00:00:00.000005Z')
                """);
    }

    // o.ts is TIMESTAMP, a.ts, a.y and b.k are TIMESTAMP_NS. s = (SELECT id, k, ts FROM b WHERE
    // ts = o.ts) holds b rows 20, 21 and 25 for o row 1 (1us), b row 23 for o row 2 (5us), and b
    // rows 22 and 24 for the NULL o.ts of o row 3, as = matches NULL to NULL. On a.ts = s.k, b row
    // 20 (1000ns) matches a rows 10 and 12, b row 25 (1500ns) a row 14, b row 23 (5000ns) a row 11,
    // and b row 22 (NULL) a row 13; b rows 21 and 24 match no a row. a row 15 (5001ns) equals no
    // outer value.
    private void createRetypedKeyTables() throws Exception {
        execute("CREATE TABLE o (id INT, ts TIMESTAMP)");
        execute("CREATE TABLE a (id INT, ts TIMESTAMP_NS, y TIMESTAMP_NS)");
        execute("CREATE TABLE b (id INT, k TIMESTAMP_NS, ts TIMESTAMP)");
        execute("INSERT INTO o VALUES (1, '2024-01-01T00:00:00.000001Z'), (2, '2024-01-01T00:00:00.000005Z'), (3, NULL)");
        execute("""
                INSERT INTO a VALUES
                (10, '2024-01-01T00:00:00.000001000Z', '2024-01-01T00:00:00.000001500Z'),
                (11, '2024-01-01T00:00:00.000005000Z', '2024-01-01T00:00:00.000007000Z'),
                (12, '2024-01-01T00:00:00.000001000Z', '2024-01-01T00:00:00.000003000Z'),
                (13, NULL, NULL),
                (14, '2024-01-01T00:00:00.000001500Z', '2024-01-01T00:00:00.000001500Z'),
                (15, '2024-01-01T00:00:00.000005001Z', '2024-01-01T00:00:00.000005001Z')
                """);
        execute("""
                INSERT INTO b VALUES
                (20, '2024-01-01T00:00:00.000001000Z', '2024-01-01T00:00:00.000001Z'),
                (21, '2024-01-01T00:00:00.000009000Z', '2024-01-01T00:00:00.000001Z'),
                (22, NULL, NULL),
                (23, '2024-01-01T00:00:00.000005000Z', '2024-01-01T00:00:00.000005Z'),
                (24, '2024-01-01T00:00:00.000000500Z', NULL),
                (25, '2024-01-01T00:00:00.000001500Z', '2024-01-01T00:00:00.000001Z')
                """);
    }

    // o.s and b.s have the outer key type, a.s and b.k the inner key type, a.y is VARCHAR. s =
    // (SELECT id, k, s FROM b WHERE s = o.s) holds b rows 20 and 21 for 'b', b row 23 for 'e', and
    // b rows 22 and 24 for the NULL o.s. On a.s = s.k, b row 20 matches a rows 10 and 12, b row 23
    // a row 11 and b row 22 a row 13.
    private void createRetypedStringKeyTables(String outerType, String innerType) throws Exception {
        execute("CREATE TABLE o (id INT, s " + outerType + ")");
        execute("CREATE TABLE a (id INT, s " + innerType + ", y VARCHAR)");
        execute("CREATE TABLE b (id INT, k " + innerType + ", s " + outerType + ")");
        execute("INSERT INTO o VALUES (1, 'b'), (2, 'e'), (3, NULL)");
        execute("INSERT INTO a VALUES (10, 'b', 'a'), (11, 'e', 'z'), (12, 'b', 'c'), (13, NULL, NULL)");
        execute("INSERT INTO b VALUES (20, 'b', 'b'), (21, 'i', 'b'), (22, NULL, NULL), (23, 'e', 'e'), (24, 'a', NULL)");
    }

    // createCorrelatedRightJoinTables() with NULL keys and ids. The NULL k of order 3 matches the
    // NULL key of xs, as = matches NULL to NULL. Trades 10 and NULL match refund 100, trade 15,
    // with a NULL x, matches refund 102, and trade 12 matches no refund.
    private void createNullableCorrelatedRightJoinTables() throws Exception {
        createCorrelatedRightJoinTables();
        execute("INSERT INTO orders VALUES (3, NULL, 3::timestamp)");
        execute("INSERT INTO trades VALUES (15, NULL, 2::timestamp), (NULL, 1, 3::timestamp), (12, 3, 4::timestamp)");
        execute("INSERT INTO refunds VALUES (102, NULL, 3::timestamp)");
        execute("INSERT INTO xs VALUES (NULL, 200)");
    }

    // Refund 2 matches no trade, so a FULL or RIGHT join of trades and refunds
    // keeps it as an unmatched row with NULL trade columns.
    private void createOrdersTradesRefundsAndXs() throws Exception {
        execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE trades (order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE refunds (order_id INT, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE xs (k INT, v INT)");
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
        execute("INSERT INTO refunds VALUES (2, 5.0, '2024-01-01T01:10:00.000000Z')");
        execute("INSERT INTO xs VALUES (1, 100)");
    }

    // For o row 3 (k = 30), LEFT JOIN pairs t row 2 with the p row (2, 30) and counts 1, while
    // ASOF and LT pick the earlier p row (2, 20) and count 0.
    private void createOuterRefJoinSlaveTables() throws Exception {
        execute("CREATE TABLE o (ok INT, k INT, v INT)");
        execute("INSERT INTO o VALUES (1, 10, 1), (2, 20, 2), (3, 30, 2)");
        execute("CREATE TABLE t (ts TIMESTAMP, id INT, v INT) TIMESTAMP(ts)");
        execute("""
                INSERT INTO t VALUES
                ('2024-01-01T00:00:01.000000Z', 1, 1),
                ('2024-01-01T00:00:03.000000Z', 2, 2),
                ('2024-01-01T00:00:05.000000Z', 3, 3)
                """);
        execute("CREATE TABLE p (ts TIMESTAMP, id INT, k INT) TIMESTAMP(ts)");
        execute("""
                INSERT INTO p VALUES
                ('2024-01-01T00:00:00.000000Z', 1, 10),
                ('2024-01-01T00:00:02.000000Z', 2, 20),
                ('2024-01-01T00:00:04.000000Z', 2, 30),
                ('2024-01-01T00:00:06.000000Z', 3, 10)
                """);
        execute("CREATE TABLE q (id INT, z INT)");
        execute("INSERT INTO q VALUES (1, 100), (2, 200), (3, 300)");
    }

    // Trade 10 matches refund 100 for o.k = 1. Refund 101 for o.k = 1, and both refunds for
    // o.k = 2, match no trade, so the RIGHT join puts false or 0 in the BOOLEAN, BYTE and SHORT
    // columns of trades, which have no NULL, and NULL in the others.
    private void createTypedCorrelatedRightJoinTables() throws Exception {
        execute("CREATE TABLE orders (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE trades (id INT, x INT, s SHORT, b BYTE, f BOOLEAN, d DOUBLE, ip IPv4, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE refunds (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO orders VALUES (1, 1, 1::timestamp), (2, 2, 2::timestamp)");
        execute("INSERT INTO trades VALUES (10, 1, 10, 10, true, 10.0, '1.1.1.1', 'abc', 1::timestamp)");
        execute("INSERT INTO refunds VALUES (100, 1, 1::timestamp), (101, 2, 2::timestamp)");
    }
}
