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

import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class LateralCorrelationTest extends AbstractCairoTest {
    private static final String FULL_JOIN_CARRIER_ROWS = """
            id\taid\tbid
            1\tnull\t21
            1\t10\t20
            2\tnull\t20
            2\tnull\t21
            2\t10\tnull
            """;
    private static final String PER_OUTER_ROW_RIGHT_JOIN = """
            id\ttid\trid
            1\tnull\t101
            1\t10\t100
            2\tnull\t100
            2\tnull\t101
            """;

    @Test
    public void testAggregateAboveOrBelowCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            assertQuery("SELECT o.id, d.c, d.s FROM o JOIN LATERAL (SELECT count() c, sum(x) s FROM t WHERE k = o.k) d ORDER BY o.id")
                    .noLeakCheck()
                    .returns("""
                            id\tc\ts
                            1\t3\t10
                            2\t3\t15
                            3\t2\t11
                            4\t0\tnull
                            """);
            assertQuery("SELECT o.id, d.s FROM o JOIN LATERAL (SELECT s FROM (SELECT k, sum(x) s FROM t) d WHERE d.k = o.k) d ORDER BY o.id")
                    .noLeakCheck()
                    .returns("""
                            id\ts
                            1\t10
                            2\t15
                            3\t11
                            """);
        });
    }

    @Test
    public void testAggregateProjectionAliases() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("""
                    SELECT m.a AS outer_a, l.*
                    FROM (SELECT 100 AS a, 200 AS b, 300 AS c) m
                    CROSS JOIN LATERAL (
                        SELECT x AS a, a+1 AS b, count() AS n FROM long_sequence(2)
                    ) l ORDER BY 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            outer_a\ta\tb\tn
                            100\t1\t2\t1
                            100\t2\t3\t1
                            """);
            assertQuery("SELECT x AS a, a+1 AS b, count() AS n FROM long_sequence(2) ORDER BY a")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tn
                            1\t2\t1
                            2\t3\t1
                            """);
            assertQuery("SELECT s.x AS a, a+s.x AS b, count() AS n FROM long_sequence(2) s ORDER BY a")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tn
                            1\t2\t1
                            2\t4\t1
                            """);
            assertQuery("""
                    SELECT m.a AS outer_a, l.*
                    FROM (SELECT 100 AS a, 200 AS b, 300 AS c) m
                    CROSS JOIN LATERAL (
                        SELECT DISTINCT x AS a, a+1 AS b, count() AS n FROM long_sequence(2)
                    ) l ORDER BY 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            outer_a\ta\tb\tn
                            100\t1\t2\t1
                            100\t2\t3\t1
                            """);
        });
    }

    @Test
    public void testAggregateProjectionAliasesInOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("""
                    SELECT DISTINCT x AS a, a+1 AS b, count() AS n
                    FROM long_sequence(2) ORDER BY a+2 DESC
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tn
                            2\t3\t1
                            1\t2\t1
                            """);
            assertQuery("""
                    SELECT l.* FROM (SELECT 100 AS a) m
                    CROSS JOIN LATERAL (
                        SELECT x AS a, count()+1 AS n FROM long_sequence(2)
                        ORDER BY a+1 DESC LIMIT 1
                    ) l
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            a\tn
                            2\t2
                            """);
            assertQuery("""
                    SELECT x AS a, count()+1 AS n
                    FROM (SELECT x, 3-x AS a FROM long_sequence(2)) ORDER BY a+1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tn
                            2\t2
                            1\t2
                            """);
        });
    }

    @Test
    public void testAggregateProjectionAliasesPreserveNullsAndSourcePrecedence() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE alias_values (x LONG)");
            execute("INSERT INTO alias_values VALUES (1), (1), (2), (NULL), (NULL)");
            assertQuery("""
                    SELECT l.* FROM (SELECT 100 AS a) m
                    CROSS JOIN LATERAL (
                        SELECT DISTINCT x AS a, a+1 AS b, coalesce(a, 99) AS c, m.a+1 AS outer_a, count() AS n
                        FROM alias_values
                    ) l ORDER BY a
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tc\touter_a\tn
                            null\tnull\t99\t101\t2
                            1\t2\t1\t101\t2
                            2\t3\t2\t101\t1
                            """);
            execute("ALTER TABLE alias_values ADD COLUMN a LONG");
            execute("INSERT INTO alias_values VALUES (3, 30)");
            assertQuery("""
                    SELECT l.* FROM (SELECT 100 AS a) m
                    CROSS JOIN LATERAL (
                        SELECT x AS a, a+1 AS b, m.a+1 AS outer_a, count() AS n FROM alias_values
                    ) l ORDER BY a
                    """)
                    .noLeakCheck()
                    .returns("""
                            a\tb\touter_a\tn
                            null\tnull\t101\t2
                            1\tnull\t101\t2
                            2\tnull\t101\t1
                            3\t31\t101\t1
                            """);
        });
    }

    @Test
    public void testAggregateProjectionAliasesPreserveValidation() throws Exception {
        assertMemoryLeak(() -> {
            assertException("SELECT x AS a, sum(a) FROM long_sequence(2)", 19, "Invalid column: a");
            assertException("SELECT count() AS a, a+1 AS b FROM long_sequence(2)", 21, "Invalid column: a");
            assertException("SELECT x+1 AS a, a+1 AS b, count() FROM long_sequence(2)", 17, "Invalid column: a");
            assertException("SELECT s.x AS a, s.a+1 AS b, count() FROM long_sequence(2) s", 17, "Invalid column: s.a");
            assertException(
                    "SELECT x AS a, a+1 AS b, count() FROM long_sequence(2) GROUP BY x",
                    15,
                    "Invalid column: a"
            );
            assertException(
                    "SELECT x AS a, x+1 AS b, count() FROM long_sequence(2) GROUP BY x+1",
                    7,
                    "column must appear in GROUP BY clause or aggregate function"
            );
        });
    }

    @Test
    public void testAsofAndLtJoinOverCorrelatedSlave() throws Exception {
        assertMemoryLeak(() -> {
            createTemporalJoinTables();
            assertOverCorrelatedSlaveAndPerKeyUnion("SELECT b.id, c.x FROM b ASOF JOIN (SELECT * FROM c WHERE k = %s) c", """
                    k\tid\tx
                    null\t1\tnull
                    null\t2\t30
                    null\t3\t31
                    null\t4\t31
                    1\t1\t10
                    1\t2\t10
                    1\t3\t12
                    1\t4\t12
                    2\t1\tnull
                    2\t2\t20
                    2\t3\t21
                    2\t4\t21
                    3\t1\tnull
                    3\t2\tnull
                    3\t3\tnull
                    3\t4\tnull
                    """);
            assertOverCorrelatedSlaveAndPerKeyUnion("SELECT b.id, c.x FROM b LT JOIN (SELECT * FROM c WHERE k = %s) c", """
                    k\tid\tx
                    null\t1\tnull
                    null\t2\t30
                    null\t3\t30
                    null\t4\t30
                    1\t1\t10
                    1\t2\t10
                    1\t3\t11
                    1\t4\t11
                    2\t1\tnull
                    2\t2\t20
                    2\t3\t21
                    2\t4\t21
                    3\t1\tnull
                    3\t2\tnull
                    3\t3\tnull
                    3\t4\tnull
                    """);
            assertOverCorrelatedSlaveAndPerKeyUnion("SELECT b.id, c.x FROM b ASOF JOIN (SELECT * FROM c WHERE k = %s) c ON (k)", """
                    k\tid\tx
                    null\t1\tnull
                    null\t2\tnull
                    null\t3\tnull
                    null\t4\t31
                    1\t1\t10
                    1\t2\tnull
                    1\t3\t12
                    1\t4\tnull
                    2\t1\tnull
                    2\t2\t20
                    2\t3\tnull
                    2\t4\tnull
                    3\t1\tnull
                    3\t2\tnull
                    3\t3\tnull
                    3\t4\tnull
                    """);
        });
    }

    @Test
    public void testAsofJoinBeforeCorrelatedRightJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE a (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO a VALUES (10, 1, '2024-01-01T00:00:01.000000Z'), (11, 2, '2024-01-01T00:00:03.000000Z')");
            execute("CREATE TABLE q (qid INT, qk INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO q VALUES
                    (50, 1, '2024-01-01T00:00:00.000000Z'),
                    (51, 2, '2024-01-01T00:00:02.000000Z'),
                    (52, 1, '2024-01-01T00:00:04.000000Z')
                    """);
            execute("CREATE TABLE r (id INT, k INT)");
            execute("INSERT INTO r VALUES (100, 1), (101, 2)");
            assertQuery("""
                    SELECT o.id, l.aid, l.qid, l.rid FROM o JOIN LATERAL (
                        SELECT a.id aid, q.qid, r.id rid
                        FROM a ASOF JOIN (SELECT qid, ts FROM q WHERE qk = o.k) q
                        RIGHT JOIN r ON r.k = a.k
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tqid\trid
                            1\t10\t50\t100
                            1\t11\t50\t101
                            2\t10\tnull\t100
                            2\t11\t51\t101
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.qid, l.rid FROM o JOIN LATERAL (
                        SELECT a.id aid, q.qid, r.id rid
                        FROM a ASOF JOIN q RIGHT JOIN r ON r.k = a.k AND r.k = o.k
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tqid\trid
                            1\tnull\tnull\t101
                            1\t10\t50\t100
                            2\tnull\tnull\t100
                            2\t11\t51\t101
                            """);
        });
    }

    @Test
    public void testCommaGroupLateralFirstInput() throws Exception {
        assertMemoryLeak(() -> {
            createCommaGroupTables();
            execute("INSERT INTO c VALUES (7)");
            assertQuery("""
                    SELECT a.k, b.k bk, c.k ck
                    FROM a, LATERAL (SELECT a.k AS k FROM long_sequence(1)) b
                    RIGHT JOIN c ON b.k=c.k ORDER BY a.k, b.k, c.k
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\tbk\tck
                            7\tnull\t1
                            7\tnull\t2
                            7\t7\t7
                            """);
            execute("CREATE TABLE d (q INT)");
            execute("INSERT INTO d VALUES (10), (20)");
            assertQuery("""
                    SELECT a.k, d.q, b.k bk, c.k ck, s.v
                    FROM a, d, LATERAL (SELECT a.k AS k FROM long_sequence(1)) b
                    RIGHT JOIN c ON b.k=c.k AND d.q>0
                    CROSS JOIN LATERAL (SELECT a.k+d.q+b.k+c.k AS v FROM long_sequence(1)) s
                    ORDER BY a.k, d.q, b.k, c.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\tq\tbk\tck\tv
                            null\tnull\tnull\t1\tnull
                            null\tnull\tnull\t2\tnull
                            7\t10\t7\t7\t31
                            7\t20\t7\t7\t41
                            """);
        });
    }

    @Test
    public void testCommaGroupLateralNestedMultiplePrefix() throws Exception {
        assertMemoryLeak(() -> {
            createCommaGroupTables();
            execute("CREATE TABLE d (q INT)");
            execute("INSERT INTO d VALUES (10), (20)");
            assertQuery("""
                    SELECT a.k, d.q, s.v FROM a, d, b RIGHT JOIN c ON b.k=c.k
                    CROSS JOIN LATERAL (
                        SELECT t.v FROM long_sequence(1) n
                        CROSS JOIN LATERAL (SELECT a.k+d.q+c.k AS v FROM long_sequence(1)) t
                    ) s ORDER BY a.k, d.q, s.v
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\tq\tv
                            7\t10\t18
                            7\t10\t19
                            7\t20\t28
                            7\t20\t29
                            """);
        });
    }

    @Test
    public void testCommaGroupLateralPreservesNullsAndDuplicates() throws Exception {
        assertMemoryLeak(() -> {
            createCommaGroupTables();
            assertQuery("""
                    SELECT a.k, s.v FROM a, b RIGHT JOIN c ON b.k=c.k
                    CROSS JOIN LATERAL (SELECT a.k AS v FROM long_sequence(1)) s
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            k\tv
                            7\t7
                            7\t7
                            """);
            execute("INSERT INTO a VALUES (8), (7), (NULL)");
            execute("INSERT INTO b VALUES (3)");
            assertQuery("""
                    SELECT a.k, b.k bk, c.k ck, s.v FROM a, b RIGHT JOIN c ON b.k=c.k
                    CROSS JOIN LATERAL (SELECT a.k+coalesce(b.k,0)+c.k AS v FROM long_sequence(1)) s
                    ORDER BY a.k, b.k, c.k, s.v
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\tbk\tck\tv
                            null\tnull\t2\tnull
                            null\t1\t1\tnull
                            7\tnull\t2\t9
                            7\tnull\t2\t9
                            7\t1\t1\t9
                            7\t1\t1\t9
                            8\tnull\t2\t10
                            8\t1\t1\t10
                            """);
            assertQuery("""
                    SELECT a.k, b.k bk, c.k ck, s.v FROM a, b FULL JOIN c ON b.k=c.k
                    CROSS JOIN LATERAL (SELECT a.k+coalesce(b.k,0)+coalesce(c.k,0) AS v FROM long_sequence(1)) s
                    ORDER BY a.k, b.k, c.k, s.v
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\tbk\tck\tv
                            null\tnull\t2\tnull
                            null\t1\t1\tnull
                            null\t3\tnull\tnull
                            7\tnull\t2\t9
                            7\tnull\t2\t9
                            7\t1\t1\t9
                            7\t1\t1\t9
                            7\t3\tnull\t10
                            7\t3\tnull\t10
                            8\tnull\t2\t10
                            8\t1\t1\t10
                            8\t3\tnull\t11
                            """);
            assertQuery("""
                    SELECT a.k, s.v FROM (SELECT * FROM a WHERE k<0) a, b RIGHT JOIN c ON b.k=c.k
                    CROSS JOIN LATERAL (SELECT a.k AS v FROM long_sequence(1)) s
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("k\tv\n");
        });
    }

    @Test
    public void testCommaGroupLateralScope() throws Exception {
        assertMemoryLeak(() -> {
            createCommaGroupTables();
            assertQuery("""
                    SELECT a.k, s.v FROM a, b RIGHT JOIN c ON b.k=c.k
                    CROSS JOIN LATERAL (SELECT a.k AS v FROM (SELECT 99 AS k) a) s
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            k\tv
                            7\t99
                            7\t99
                            """);
            assertException(
                    "SELECT a.k FROM a, (SELECT a.k AS k FROM long_sequence(1)) b RIGHT JOIN c ON b.k=c.k",
                    27,
                    "Invalid table name or alias"
            );
            final String ambiguous = """
                    SELECT s.v FROM a, b RIGHT JOIN (SELECT 1 AS j) c ON b.k=c.j
                    CROSS JOIN LATERAL (SELECT k AS v FROM long_sequence(1)) s
                    """;
            assertException(ambiguous, ambiguous.indexOf("k AS v"), "Invalid column: k");
        });
    }

    @Test
    public void testConsecutiveRightAndFullJoinsAfterCorrelatedOn() throws Exception {
        // #7723
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            execute("INSERT INTO xs VALUES (3, 300)");
            assertQuery("""
                    SELECT o.id, l.tid, l.rid, l.xk FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid, x.k xk FROM trades t
                        RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        RIGHT JOIN xs x ON x.k = r.k
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid\txk
                            1\tnull\tnull\t3
                            1\t10\t100\t1
                            2\tnull\tnull\t3
                            2\tnull\t100\t1
                            """);
            execute("INSERT INTO xs VALUES (2, 200)");
            assertQuery("""
                    SELECT o.id, l.tid, l.rid, l.xk FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid, x.k xk FROM trades t
                        FULL JOIN refunds r ON t.x = r.k AND r.k = o.k
                        RIGHT JOIN xs x ON x.k = r.k
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid\txk
                            1\tnull\tnull\t3
                            1\tnull\t101\t2
                            1\t10\t100\t1
                            2\tnull\tnull\t3
                            2\tnull\t100\t1
                            2\tnull\t101\t2
                            """);
        });
    }

    @Test
    public void testCorrelatedFromSubQueryWithWhereEquality() throws Exception {
        // #7727
        assertMemoryLeak(() -> {
            createOuterKeyed("(1, 1, 1), (2, 2, 2)");
            createAKeyed("(10, 1, 1), (11, 2, 2)");
            assertQuery("""
                    SELECT o.id, l.sx FROM o JOIN LATERAL (
                        SELECT s.x sx FROM (SELECT id, k, x FROM a WHERE k = o.k) s WHERE s.x = o.x
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tsx
                            1\t1
                            2\t2
                            """);
            assertQuery("""
                    SELECT o.id, l.sx FROM o JOIN LATERAL (
                        SELECT s.x sx FROM (SELECT id, k, x FROM a WHERE k = o.k) s WHERE s.x = o.k
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tsx
                            1\t1
                            2\t2
                            """);
        });
    }

    @Test
    public void testCorrelatedLimitWithNullBound() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT)");
            execute("INSERT INTO a VALUES (1), (2), (null)");
            execute("CREATE TABLE b (id INT, k INT)");
            execute("INSERT INTO b VALUES (1, 1), (2, 1), (3, 1), (4, 2), (5, 2), (6, null), (7, null)");
            assertQuery("SELECT a.k, l.id FROM a JOIN LATERAL (SELECT id FROM b WHERE b.k = a.k ORDER BY id LIMIT a.k) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .returns("""
                            k\tid
                            null\t6
                            null\t7
                            1\t1
                            2\t4
                            2\t5
                            """);
            assertQuery("SELECT a.k, l.id FROM a JOIN LATERAL (SELECT id FROM b ORDER BY id LIMIT a.k) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .returns("""
                            k\tid
                            null\t1
                            null\t2
                            null\t3
                            null\t4
                            null\t5
                            null\t6
                            null\t7
                            1\t1
                            2\t1
                            2\t2
                            """);
            assertQuery("SELECT a.k, l.id FROM a JOIN LATERAL (SELECT id FROM b ORDER BY id LIMIT a.k, 3) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .returns("""
                            k\tid
                            null\t1
                            null\t2
                            null\t3
                            1\t2
                            1\t3
                            2\t3
                            """);
            assertQuery("SELECT a.k, l.id FROM a LEFT JOIN LATERAL (SELECT id FROM b ORDER BY id LIMIT 1, a.k) l ON true ORDER BY 1, 2")
                    .noLeakCheck()
                    .returns("""
                            k\tid
                            null\tnull
                            1\tnull
                            2\t2
                            """);
            assertQuery("SELECT a.k, l.m FROM a LEFT JOIN LATERAL (SELECT max(id) m FROM b WHERE b.k = a.k LIMIT a.k) l ON true ORDER BY 1")
                    .noLeakCheck()
                    .returns("""
                            k\tm
                            null\t7
                            1\t3
                            2\t5
                            """);
            assertQuery("SELECT a.k, l.m FROM a JOIN LATERAL (SELECT max(id) m FROM b WHERE b.k = a.k LIMIT 1, a.k) l ORDER BY 1")
                    .noLeakCheck()
                    .returns("""
                            k\tm
                            """);
        });
    }

    @Test
    public void testCorrelatedPredicateWithSubQuery() throws Exception {
        // #7803 section 16
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, v INT)");
            execute("INSERT INTO o VALUES (1, 1, 5), (2, 2, 50)");
            execute("CREATE TABLE b (k INT, v INT)");
            execute("INSERT INTO b VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE c (x INT)");
            execute("INSERT INTO c VALUES (1), (2)");
            assertQuery("""
                    SELECT o.id, t.v FROM o CROSS JOIN LATERAL (
                        SELECT b.v FROM b WHERE b.k = o.k AND b.v - o.v > (SELECT count() FROM c)
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tv
                            1\t10
                            """);
            assertQuery("""
                    SELECT o.id, t.v FROM o CROSS JOIN LATERAL (
                        SELECT b.v FROM b WHERE b.k = o.k AND o.v > (SELECT count() FROM c)
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tv
                            1\t10
                            2\t20
                            """);
            assertQuery("""
                    SELECT o.id, t.v FROM o LEFT JOIN LATERAL (
                        SELECT b.v FROM b WHERE b.k = o.k AND b.v - o.v > (SELECT count() FROM c)
                    ) t ON true ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t10
                            2\tnull
                            """);
        });
    }

    @Test
    public void testCorrelatedSourceKinds() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            assertQuery("""
                    SELECT o.id, d.x FROM o JOIN LATERAL (
                        SELECT a.x FROM t a JOIN t b ON a.x = b.x AND b.k = o.k ORDER BY a.x LIMIT 1
                    ) d ORDER BY o.id, d.x
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tx
                            1\t1
                            2\t3
                            3\t5
                            """);
            assertQuery("SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM t WHERE k = o.k UNION SELECT x FROM t WHERE x = o.id) d ORDER BY o.id, d.x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tx
                            1\t1
                            1\t2
                            1\t7
                            2\t2
                            2\t3
                            2\t4
                            2\t8
                            3\t3
                            3\t5
                            3\t6
                            4\t4
                            """);
            assertQuery("""
                    SELECT o.id, d.x, d.b FROM o JOIN LATERAL (
                        SELECT e.x, sum(q.bid) b FROM e WINDOW JOIN q ON (s) RANGE BETWEEN 1 hour PRECEDING AND CURRENT ROW WHERE e.k = o.k
                    ) d ORDER BY o.id, d.x
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tx\tb
                            1\t1\tnull
                            1\t2\tnull
                            1\t7\t30.0
                            2\t3\t10.0
                            2\t4\t20.0
                            2\t8\t40.0
                            3\t5\t40.0
                            3\t6\t20.0
                            """);
            assertQuery("""
                    SELECT o.id, d.n, d.b FROM o JOIN LATERAL (
                        SELECT count() n, sum(q.bid) b FROM e HORIZON JOIN q ON (s) LIST (0) AS h WHERE e.k = o.k
                    ) d ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tn\tb
                            1\t3\t30.0
                            2\t3\t70.0
                            3\t2\t50.0
                            4\t0\tnull
                            """);
            execute("CREATE TABLE arrs (id INT, arr DOUBLE[])");
            execute("INSERT INTO arrs VALUES (1, ARRAY[1.0, 2.0]), (2, ARRAY[3.0])");
            assertQuery("""
                    SELECT o.id, d.x, d.v FROM arrs o JOIN LATERAL (
                        SELECT t.x, u.v FROM t CROSS JOIN UNNEST(o.arr) u(v) WHERE t.x = o.id
                    ) d ORDER BY o.id, d.x, d.v
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tx\tv
                            1\t1\t1.0
                            1\t1\t2.0
                            2\t2\t3.0
                            """);
            final String functionSource = "SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM long_sequence(o.id)) d";
            assertQuery(functionSource).noLeakCheck().fails(functionSource.indexOf("o.id)"), "constant expected");
        });
    }

    @Test
    public void testCorrelatedSubQueryAroundRightOrFullJoin() throws Exception {
        // #7724
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.tid, l.rid, l.xk FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid, x.k xk
                        FROM trades t FULL JOIN refunds r ON r.k = t.x
                        LEFT JOIN (SELECT k, v FROM xs WHERE k = o.k) x ON x.v > 0
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid\txk
                            1\tnull\t101\t1
                            1\t10\t100\t1
                            2\tnull\t101\tnull
                            2\t10\t100\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid
                        FROM trades t FULL JOIN (SELECT id, k FROM refunds WHERE k = o.k) r ON r.k = t.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            2\tnull\t101
                            2\t10\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid
                        FROM (SELECT id, x FROM trades WHERE x = o.k) t RIGHT JOIN refunds r ON t.x = r.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid
                        FROM trades t JOIN (SELECT k, v FROM xs WHERE k = o.k) x ON x.k = t.x
                        RIGHT JOIN refunds r ON r.k = t.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid
                        FROM (SELECT id, x FROM trades WHERE x = o.k) t
                        FULL JOIN (SELECT id, k FROM refunds WHERE k = o.k) r ON r.k = t.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            2\tnull\t101
                            """);
        });
    }

    @Test
    public void testCorrelatedUnionAllOnLeftJoinSlave() throws Exception {
        // #7803 section 5
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, 2)");
            createAKeyOnly("(10, 1), (11, 2)");
            createB("(20, 1, 1), (21, 2, 2)");
            execute("CREATE TABLE c (id INT, k INT, x INT)");
            execute("INSERT INTO c VALUES (30, 1, 1), (31, 2, 2)");
            assertQuery("""
                    SELECT o.id, t.aid, t.rid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id rid FROM a
                        LEFT JOIN (
                            SELECT id, k FROM b WHERE x = o.x
                            UNION ALL
                            SELECT id, k FROM c WHERE x = o.x
                        ) r ON r.k = a.k
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\trid
                            1\t10\t20
                            1\t10\t30
                            1\t11\tnull
                            2\t10\tnull
                            2\t11\t21
                            2\t11\t31
                            """);
            assertQuery("""
                    SELECT o.id, t.aid, t.rid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id rid FROM a
                        JOIN (
                            SELECT id, k FROM b WHERE x = o.x
                            UNION ALL
                            SELECT id, k FROM c WHERE x = o.x
                        ) r ON r.k = a.k
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\trid
                            1\t10\t20
                            1\t10\t30
                            2\t11\t21
                            2\t11\t31
                            """);
        });
    }

    @Test
    public void testCorrelationAtOrBeforeSpliceJoinRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, g GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE a (id INT, g GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE b (id INT, g GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO o VALUES (1, #u33d8, '2024-01-01T00:00:01.000000Z'), (2, #v33d8, '2024-01-01T00:00:02.000000Z')");
            execute("INSERT INTO a VALUES (10, #u33d8, '2024-01-01T00:00:01.000000Z'), (11, #v33d8, '2024-01-01T00:00:03.000000Z')");
            execute("INSERT INTO b VALUES (20, #u33d8, '2024-01-01T00:00:02.000000Z'), (21, #w33d8, '2024-01-01T00:00:04.000000Z')");
            assertSpliceCorrelationRejected("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM (SELECT * FROM a WHERE id > o.id + 8) a SPLICE JOIN b
                    ) l
                    """);
            assertSpliceCorrelationRejected("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a SPLICE JOIN (SELECT * FROM b WHERE id > o.id + 18) b
                    ) l
                    """);
            assertSpliceCorrelationRejected("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a JOIN b ON b.id = o.id + 19 SPLICE JOIN b b2
                    ) l
                    """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a SPLICE JOIN b WHERE b.g = o.g
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            1\t11\t20
                            """);
        });
    }

    @Test
    public void testCountOverRightJoinWithEmptyPreservedSide() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            execute("INSERT INTO orders VALUES (3, 3)");
            assertQuery("""
                    SELECT o.id, l.c FROM orders o LEFT JOIN LATERAL (
                        SELECT count(*) c FROM trades t RIGHT JOIN (SELECT id, k FROM refunds WHERE k = o.k) r ON t.x = r.k
                    ) l ON true ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc
                            1\t1
                            2\t1
                            3\t0
                            """);
            assertQuery("""
                    SELECT o.id, l.c FROM orders o JOIN LATERAL (
                        SELECT count(*) c FROM trades t RIGHT JOIN (SELECT id, k FROM refunds WHERE k = o.k) r ON t.x = r.k
                    ) l ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc
                            1\t1
                            2\t1
                            3\t0
                            """);
        });
    }

    @Test
    public void testDistinctAboveOrBelowCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            assertQuery("SELECT o.id, d.s FROM o JOIN LATERAL (SELECT DISTINCT sum(x) s FROM t WHERE x > o.id GROUP BY k) d ORDER BY o.id, d.s")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ts
                            1\t9
                            1\t11
                            1\t15
                            2\t7
                            2\t11
                            2\t15
                            3\t7
                            3\t11
                            3\t12
                            4\t7
                            4\t8
                            4\t11
                            """);
            assertQuery("SELECT o.id, d.s FROM o JOIN LATERAL (SELECT s FROM (SELECT DISTINCT sum(x) s, k FROM t GROUP BY k) d WHERE d.k = o.k) d ORDER BY o.id, d.s")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ts
                            1\t10
                            2\t15
                            3\t11
                            """);
        });
    }

    @Test
    public void testDistinctProjectionAliases() throws Exception {
        allowFunctionMemoization();
        assertMemoryLeak(() -> {
            assertQuery("""
                    SELECT m.a, l.a AS la, l.b FROM (SELECT 100 AS a) m
                    CROSS JOIN LATERAL (
                        SELECT DISTINCT x AS a, a+1 AS b FROM long_sequence(2)
                    ) l ORDER BY la
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tla\tb
                            100\t1\t2
                            100\t2\t3
                            """);
            execute("CREATE TABLE alias_values (x LONG)");
            execute("INSERT INTO alias_values VALUES (1), (1), (2), (NULL), (NULL)");
            assertQuery("""
                    SELECT m.a, l.a AS la, l.b, l.c, l.d FROM (SELECT 100 AS a, 200 AS b) m
                    CROSS JOIN LATERAL (
                        SELECT DISTINCT x AS a, a AS b, b+1 AS c, c+1 AS d FROM alias_values
                    ) l ORDER BY la
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tla\tb\tc\td
                            100\tnull\tnull\tnull\tnull
                            100\t1\t1\t2\t3
                            100\t2\t2\t3\t4
                            """);
            assertQuery("""
                    SELECT DISTINCT x AS a, a AS b, b+1 AS c FROM alias_values
                    ORDER BY c+1 DESC LIMIT 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tc
                            2\t2\t3
                            1\t1\t2
                            """);
        });
    }

    @Test
    public void testDistinctProjectionAliasesPreserveSourcePrecedence() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE alias_values (x LONG, a LONG)");
            execute("INSERT INTO alias_values VALUES (1, 10), (1, 10), (2, NULL), (NULL, 30)");
            assertQuery("""
                    SELECT m.a, l.a AS la, l.b, l.c, l.outer_a FROM (SELECT 100 AS a, 200 AS b) m
                    CROSS JOIN LATERAL (
                        SELECT DISTINCT x AS a, a AS b, b+1 AS c, m.a+1 AS outer_a FROM alias_values
                    ) l ORDER BY la
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tla\tb\tc\touter_a
                            100\tnull\t30\t31\t101
                            100\t1\t10\t11\t101
                            100\t2\tnull\tnull\t101
                            """);
        });
    }

    @Test
    public void testDomainOfAsofJoinedOuterColumnHoldsNull() throws Exception {
        // #7803 section 11
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO o VALUES (1, 1, '2024-01-01T00:00:01.000000Z'), (2, 2, '2024-01-01T00:00:02.000000Z')");
            execute("CREATE TABLE c (v INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO c VALUES (5, 1, '2024-01-01T00:00:00.000000Z')");
            execute("CREATE TABLE d (v INT)");
            execute("INSERT INTO d VALUES (3), (7)");
            assertQuery("""
                    SELECT o.id, c.v cv, t.dv FROM o ASOF JOIN c ON (x) CROSS JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(c.v, 0)
                    ) t ORDER BY 1, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcv\tdv
                            1\t5\t7
                            2\tnull\t3
                            2\tnull\t7
                            """);
        });
    }

    @Test
    public void testDomainOfLeftJoinedOuterColumnHoldsNull() throws Exception {
        // #7803 section 11
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, 2)");
            execute("CREATE TABLE c (v INT, x INT)");
            execute("INSERT INTO c VALUES (5, 1)");
            execute("CREATE TABLE d (v INT)");
            execute("INSERT INTO d VALUES (3), (7)");
            assertQuery("""
                    SELECT o.id, c.v cv, t.dv FROM o LEFT JOIN c ON c.x = o.x CROSS JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(c.v, 0)
                    ) t ORDER BY 1, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcv\tdv
                            1\t5\t7
                            2\tnull\t3
                            2\tnull\t7
                            """);
            assertQuery("""
                    SELECT o.id, t1.v cv, t2.dv FROM o LEFT JOIN LATERAL (
                        SELECT v FROM c WHERE x = o.x
                    ) t1 ON true CROSS JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(t1.v, 0)
                    ) t2 ORDER BY 1, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcv\tdv
                            1\t5\t7
                            2\tnull\t3
                            2\tnull\t7
                            """);
            assertQuery("""
                    SELECT o.id, c.v cv, t.dv FROM o LEFT JOIN (SELECT v, x FROM c) c ON c.x = o.x CROSS JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(c.v, 0)
                    ) t ORDER BY 1, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcv\tdv
                            1\t5\t7
                            2\tnull\t3
                            2\tnull\t7
                            """);
            assertQuery("""
                    SELECT o.id, c.v cv, t.dv FROM o LEFT JOIN c ON c.x = o.x LEFT JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(c.v, 0)
                    ) t ON true ORDER BY 1, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcv\tdv
                            1\t5\t7
                            2\tnull\t3
                            2\tnull\t7
                            """);
            assertQuery("""
                    SELECT o.id, t1.v cv, t2.dv FROM o LEFT JOIN LATERAL (
                        SELECT v FROM c WHERE x = o.x
                    ) t1 ON true LEFT JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(t1.v, 0)
                    ) t2 ON true ORDER BY 1, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcv\tdv
                            1\t5\t7
                            2\tnull\t3
                            2\tnull\t7
                            """);
            assertQuery("""
                    SELECT o.id, c.v cv, t.n FROM o LEFT JOIN c ON c.x = o.x CROSS JOIN LATERAL (
                        SELECT count(*) n FROM d WHERE v > coalesce(c.v, 0)
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcv\tn
                            1\t5\t1
                            2\tnull\t2
                            """);
            assertQuery("""
                    SELECT o.id, t1.v cv, t2.n FROM o LEFT JOIN LATERAL (
                        SELECT v FROM c WHERE x = o.x
                    ) t1 ON true CROSS JOIN LATERAL (
                        SELECT count(*) n FROM d WHERE v > coalesce(t1.v, 0)
                    ) t2 ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcv\tn
                            1\t5\t1
                            2\tnull\t2
                            """);
        });
    }

    @Test
    public void testDomainOfLeftJoinedOuterColumnHoldsNullForEveryKeyType() throws Exception {
        // #7803 section 11
        assertMemoryLeak(() -> {
            assertNullOuterValueFindsDomainRow("INT", "1", "2");
            assertNullOuterValueFindsDomainRow("LONG", "1", "2");
            assertNullOuterValueFindsDomainRow("DOUBLE", "1.5", "2.5");
            assertNullOuterValueFindsDomainRow("FLOAT", "1.5", "2.5");
            assertNullOuterValueFindsDomainRow("STRING", "'a'", "'b'");
            assertNullOuterValueFindsDomainRow("VARCHAR", "'a'", "'b'");
            assertNullOuterValueFindsDomainRow("SYMBOL", "'a'", "'b'");
            assertNullOuterValueFindsDomainRow("CHAR", "'a'", "'b'");
            assertNullOuterValueFindsDomainRow("UUID", "'11111111-1111-1111-1111-111111111111'", "'22222222-2222-2222-2222-222222222222'");
            assertNullOuterValueFindsDomainRow("IPV4", "'1.1.1.1'", "'2.2.2.2'");
            assertNullOuterValueFindsDomainRow("LONG256", "'0x01'", "'0x02'");
            assertNullOuterValueFindsDomainRow("DATE", "'2024-01-01T00:00:00.000Z'", "'2024-01-02T00:00:00.000Z'");
            assertNullOuterValueFindsDomainRow("TIMESTAMP", "'2024-01-01T00:00:00.000000Z'", "'2024-01-02T00:00:00.000000Z'");
            assertNullOuterValueFindsDomainRow("TIMESTAMP_NS", "'2024-01-01T00:00:00.000000000Z'", "'2024-01-02T00:00:00.000000000Z'");
        });
    }

    @Test
    public void testDomainOfOuterColumnNulledByRightOrFullJoin() throws Exception {
        // #7803 section 11
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, 2)");
            execute("CREATE TABLE c (v INT, x INT)");
            execute("INSERT INTO c VALUES (5, 1), (6, 9)");
            execute("CREATE TABLE d (v INT)");
            execute("INSERT INTO d VALUES (3), (7)");
            assertQuery("""
                    SELECT o.id, c.v cv, t.dv FROM o RIGHT JOIN c ON c.x = o.x CROSS JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(o.x, 0) + 1
                    ) t ORDER BY 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcv\tdv
                            1\t5\t3
                            1\t5\t7
                            null\t6\t3
                            null\t6\t7
                            """);
            assertQuery("""
                    SELECT o.id, c.v cv, t.dv FROM o FULL JOIN c ON c.x = o.x CROSS JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(o.x, 0) + 1
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcv\tdv
                            null\t6\t3
                            null\t6\t7
                            1\t5\t3
                            1\t5\t7
                            2\tnull\t7
                            """);
        });
    }

    @Test
    public void testDuplicatedOnConjunctOfOuterJoinInBody() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            final String plan = """
                    Sort
                      keys: [id, tid, rid]
                      Project
                        columns: [o.id, l.tid, l.rid]
                        Join
                          Master o
                            Scan
                              table: orders
                              columns: [id, k]
                          INNER l
                            keys: [l.__qdb_outer_ref__0_k = o.k]
                            Project
                              columns: [t.id AS tid, r.id AS rid, r.__qdb_outer_ref__0_k]
                              Join
                                Master t
                                  Scan
                                    table: trades
                                    columns: [id, x]
                                RIGHT r
                                  keys: [r.k = t.x]
                                  on: r.k = r.__qdb_outer_ref__0_k
                                  Join
                                    Master
                                      Scan
                                        table: refunds
                                        columns: [id, k]
                                    CROSS __qdb_outer_ref__0
                                      Aggregate
                                        keys: [k AS __qdb_outer_ref__0_k]
                                        values: []
                                        Scan
                                          table: orders
                                          columns: [k]
                    """;
            final String single = """
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """;
            final String duplicated = """
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND t.x = r.k AND r.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """;
            assertQuery(single).noLeakCheck().assertsLogicalPlan(plan);
            assertQuery(duplicated).noLeakCheck().assertsLogicalPlan(plan);
            assertQuery(duplicated).noLeakCheck().expectSize().returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k AND r.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
        });
    }

    @Test
    public void testEquatedInnerColumnsFollowedByLeftJoinsWithRedundantKeys() throws Exception {
        // #7731
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (k INT, x INT, y INT, v INT)");
            execute("CREATE TABLE tb (k INT, x INT, y INT, v INT)");
            execute("CREATE TABLE tc (k INT, x INT, y INT, v INT)");
            execute("CREATE TABLE td (k INT, x INT, y INT, v INT)");
            execute("INSERT INTO ta VALUES (null, 1, 1, 101), (1, 0, 3, 102), (2, 1, null, 103), (0, 1, 1, 104), (3, 0, 2, 105), (1, 3, 0, 106)");
            execute("INSERT INTO tb VALUES (0, 1, 1, 201), (0, 1, 1, 202), (null, null, 0, 203), (2, 3, 2, 204), (0, 2, 3, 205)");
            execute("INSERT INTO tc VALUES (2, 0, null, 301), (3, 2, 3, 302), (2, 3, null, 303), (3, 3, 3, 304), (0, 3, 0, 305)");
            execute("INSERT INTO td VALUES (0, null, null, 401), (0, 3, 0, 402), (0, null, 1, 403), (1, 3, 0, 404), (null, 1, 1, 405), (2, 0, 1, 406)");
            assertQuery("""
                    SELECT a.v av, t.v tv, c.v cv, d.v dv
                    FROM ta a
                    JOIN LATERAL (
                        SELECT k kk, x xx, y yy, v FROM tb WHERE k = a.x AND x = a.x
                    ) t ON t.yy = a.y
                    LEFT JOIN tc c ON c.k = t.kk AND c.k = t.xx
                    LEFT JOIN td d ON t.yy = d.y AND t.yy = d.y
                    WHERE a.x = 1
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            av\ttv\tcv\tdv
                            """);
        });
    }

    @Test
    public void testFailedCompilationFreesSharedSource() throws Exception {
        // #7835
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("CREATE TABLE a (id INT, k INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            final String sql1 = """
                    SELECT o.id, l.aid, l.rid, l.sid
                    FROM o
                    JOIN LATERAL (
                        SELECT a.id aid, r.id rid, s.id sid
                        FROM a
                        JOIN (SELECT id, k FROM b WHERE x != o.x) r ON r.k = a.k
                        ASOF JOIN b s
                    ) l
                    """;
            assertExceptionNoLeakCheck(sql1, sql1.indexOf("ASOF JOIN b s"), "left side of time series join has no timestamp");
            final String sql2 = """
                    EXPLAIN SELECT o.id, l.aid, l.rid, l.sid
                    FROM o
                    JOIN LATERAL (
                        SELECT a.id aid, r.id rid, s.id sid
                        FROM a
                        JOIN (SELECT id, k FROM b WHERE x != o.x) r ON r.k = a.k
                        ASOF JOIN b s
                    ) l
                    """;
            assertExceptionNoLeakCheck(sql2, sql2.indexOf("ASOF JOIN b s"), "left side of time series join has no timestamp");
        });
    }

    @Test
    public void testFillAboveOrBelowCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            final String expected = """
                    id\tts\tc
                    1\t2024-01-01T00:00:00.000000Z\t2
                    1\t2024-01-01T02:00:00.000000Z\tnull
                    1\t2024-01-01T04:00:00.000000Z\tnull
                    1\t2024-01-01T06:00:00.000000Z\t1
                    2\t2024-01-01T00:00:00.000000Z\tnull
                    2\t2024-01-01T02:00:00.000000Z\t2
                    2\t2024-01-01T04:00:00.000000Z\tnull
                    2\t2024-01-01T06:00:00.000000Z\t1
                    3\t2024-01-01T00:00:00.000000Z\tnull
                    3\t2024-01-01T02:00:00.000000Z\tnull
                    3\t2024-01-01T04:00:00.000000Z\t2
                    3\t2024-01-01T06:00:00.000000Z\tnull
                    """;
            assertQuery("SELECT o.id, d.ts, d.c FROM o JOIN LATERAL (SELECT ts, count() c FROM e WHERE k = o.k SAMPLE BY 2h FILL(NULL)) d ORDER BY o.id, d.ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("""
                    SELECT o.id, d.ts, d.c FROM o JOIN LATERAL (
                        SELECT ts, c FROM (SELECT ts, k, count() c FROM e SAMPLE BY 2h FILL(NULL)) d WHERE d.k = o.k
                    ) d ORDER BY o.id, d.ts
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            final String everyBucketOfFirstRow = """
                    id\tts\tc
                    1\t2024-01-01T00:00:00.000000Z\t2
                    1\t2024-01-01T02:00:00.000000Z\t2
                    1\t2024-01-01T04:00:00.000000Z\t2
                    1\t2024-01-01T06:00:00.000000Z\t2
                    """;
            assertQuery("""
                    SELECT o.id, d.ts, d.c FROM o JOIN LATERAL (
                        SELECT ts, c FROM (SELECT ts, count() c FROM e SAMPLE BY 2h FILL(NULL)) d WHERE d.c > o.id
                    ) d ORDER BY o.id, d.ts
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(everyBucketOfFirstRow);
            assertQuery("""
                    SELECT o.id, d.ts, d.c FROM o JOIN LATERAL (
                        SELECT * FROM (SELECT ts, count() c FROM e SAMPLE BY 2h FILL(NULL)) d WHERE d.c > o.id
                    ) d ORDER BY o.id, d.ts
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(everyBucketOfFirstRow);
        });
    }

    @Test
    public void testFilterAboveOrBelowCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            assertQuery("SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM t WHERE k = o.k AND x > o.id) d ORDER BY o.id, d.x")
                    .noLeakCheck()
                    .returns("""
                            id\tx
                            1\t2
                            1\t7
                            2\t3
                            2\t4
                            2\t8
                            3\t5
                            3\t6
                            """);
            assertQuery("SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM (SELECT x, k FROM t WHERE x > 2) d WHERE d.k = o.k) d ORDER BY o.id, d.x")
                    .noLeakCheck()
                    .returns("""
                            id\tx
                            1\t7
                            2\t3
                            2\t4
                            2\t8
                            3\t5
                            3\t6
                            """);
        });
    }

    @Test
    public void testFullJoinCarrierForEveryOuterColumnType() throws Exception {
        assertMemoryLeak(() -> {
            assertFullJoinCarrier("BOOLEAN", "false", "true");
            assertFullJoinCarrier("BYTE", "0::byte", "1::byte");
            assertFullJoinCarrier("SHORT", "0::short", "1::short");
            assertFullJoinCarrier("CHAR", "'a'", "'b'");
            assertFullJoinCarrier("GEOHASH(5c)", "#u33d8", "#v33d8");
            assertFullJoinCarrier("GEOHASH(1c)", "#u", "#v");
            assertFullJoinCarrier("INT", "1", "2");
            assertFullJoinCarrier("LONG", "1", "2");
            assertFullJoinCarrier("DOUBLE", "1.5", "2.5");
            assertFullJoinCarrier("SYMBOL", "'s1'", "'s2'");
            assertFullJoinCarrier("VARCHAR", "'v1'", "'v2'");
            assertFullJoinCarrier("STRING", "'t1'", "'t2'");
            assertFullJoinCarrier("IPv4", "'1.1.1.1'", "'2.2.2.2'");
            assertFullJoinCarrier("UUID", "'11111111-1111-1111-1111-111111111111'", "'22222222-2222-2222-2222-222222222222'");
            assertFullJoinCarrier("LONG256", "'0x01'", "'0x02'");
            assertFullJoinCarrier("DECIMAL(18,2)", "1.5m", "2.5m");
            assertFullJoinCarrier("DECIMAL(38,2)", "1.5m", "2.5m");
            assertFullJoinCarrier("DATE", "'2024-01-01'", "'2024-01-02'");
            assertFullJoinCarrier("TIMESTAMP", "'2024-01-01T00:00:00.000001Z'", "'2024-01-02T00:00:00.000001Z'");
            assertFullJoinCarrier("TIMESTAMP_NS", "'2024-01-01T00:00:00.000000001Z'", "'2024-01-02T00:00:00.000000002Z'");
        });
    }

    @Test
    public void testFullJoinCarrierForIntervalOuterColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO o VALUES (1, '2024-01-01T00:00:01.000000Z'), (2, '2024-01-01T00:00:02.000000Z')");
            execute("CREATE TABLE a (id INT, ts TIMESTAMP)");
            execute("INSERT INTO a VALUES (10, '2024-01-01T00:00:01.000000Z')");
            execute("CREATE TABLE b (id INT, ts TIMESTAMP)");
            execute("INSERT INTO b VALUES (20, '2024-01-01T00:00:01.000000Z'), (21, '2024-01-01T00:00:02.000000Z')");
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM (SELECT id, interval(ts, ts) iv FROM o) o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a FULL JOIN b ON a.ts = b.ts AND b.ts IN o.iv
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(FULL_JOIN_CARRIER_ROWS);
        });
    }

    @Test
    public void testFullJoinOnReadingOuterColumn() throws Exception {
        // #7723, #7730
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            final String expected = """
                    id\ttid\trid
                    1\tnull\t101
                    1\t10\t100
                    2\tnull\t100
                    2\tnull\t101
                    2\t10\tnull
                    """;
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t FULL JOIN refunds r ON r.k = o.k AND t.x = r.k AND t.id > 0
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t FULL JOIN refunds r ON r.k = o.k AND t.x = r.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("""
                    SELECT o.id, l.c, l.ct FROM orders o JOIN LATERAL (
                        SELECT count(*) c, count(t.id) ct FROM trades t FULL JOIN refunds r ON r.k = o.k AND t.x = r.k
                    ) l ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc\tct
                            1\t2\t1
                            2\t3\t1
                            """);
            createOuterKeyed("(1, 1, 1), (2, 2, 2)");
            createAKeyed("(10, 1, 1), (11, 2, 2)");
            createB("(20, 1, 1), (21, 2, 3)");
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a FULL JOIN b ON a.x = b.k AND b.k = o.k WHERE a.id > 0
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            1\t11\tnull
                            2\t10\tnull
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a FULL JOIN b ON a.x = b.k AND b.k = o.k WHERE a.id > 0 AND b.id > 0
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t11\t21
                            """);
        });
    }

    @Test
    public void testFullJoinPlanSplitsWithMarkerCarrier() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t FULL JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id > 0
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .assertsLogicalPlan("""
                            Sort
                              keys: [id, tid, rid]
                              Project
                                columns: [o.id, l.tid, l.rid]
                                Join
                                  Master o
                                    Scan
                                      table: orders
                                      columns: [id, k]
                                  INNER l
                                    keys: [l.__qdb_outer_ref__0_k = o.k]
                                    Project
                                      columns: [id AS tid, id1 AS rid, __qdb_outer_ref__0_k]
                                      Project
                                        columns: [t.id, r.id AS id1, case(t.__qdb_outer_ref__marker_0 = null, r.__qdb_outer_ref__0_k, t.__qdb_outer_ref__0_k) AS __qdb_outer_ref__0_k]
                                        Filter
                                          predicate: t.id > 0
                                          Join
                                            Master t
                                              Join
                                                Master
                                                  Scan
                                                    table: trades
                                                    columns: [id, x]
                                                CROSS __qdb_outer_ref__0
                                                  Aggregate
                                                    keys: [k AS __qdb_outer_ref__0_k]
                                                    values: [count() AS __qdb_outer_ref__marker_0]
                                                    Scan
                                                      table: orders
                                                      columns: [k]
                                            FULL r
                                              keys: [r.k = t.x, r.__qdb_outer_ref__0_k = t.__qdb_outer_ref__0_k]
                                              on: r.k = r.__qdb_outer_ref__0_k
                                              Join
                                                Master
                                                  Scan
                                                    table: refunds
                                                    columns: [id, k]
                                                CROSS __qdb_outer_ref__0_1
                                                  Aggregate
                                                    keys: [k AS __qdb_outer_ref__0_k]
                                                    values: []
                                                    Scan
                                                      table: orders
                                                      columns: [k]
                            """);
        });
    }

    @Test
    public void testGroupByLocalColumnShadowsOuterReference() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE outer_t (id INT, k INT)");
            execute("CREATE TABLE inner_t (k INT, x INT)");

            final String qualified = """
                    SELECT * FROM outer_t o CROSS JOIN LATERAL (
                        SELECT i.k v, count() n FROM inner_t i WHERE i.k = o.k GROUP BY i.x
                    ) d
                    """;
            assertQuery(qualified).noLeakCheck().fails(qualified.indexOf("i.k"), "column must appear in GROUP BY clause or aggregate function");
            final String unqualified = """
                    SELECT * FROM outer_t o CROSS JOIN LATERAL (
                        SELECT k v, count() n FROM inner_t i WHERE i.k = o.k GROUP BY i.x
                    ) d
                    """;
            assertQuery(unqualified).noLeakCheck().fails(unqualified.indexOf("k v"), "column must appear in GROUP BY clause or aggregate function");
            final String shadowedAlias = """
                    SELECT * FROM outer_t o CROSS JOIN LATERAL (
                        SELECT o.k v, count() n FROM inner_t o GROUP BY o.x
                    ) d
                    """;
            assertQuery(shadowedAlias).noLeakCheck().fails(shadowedAlias.indexOf("o.k"), "column must appear in GROUP BY clause or aggregate function");
            assertQuery("SELECT i.k, count() FROM inner_t i GROUP BY i.x")
                    .noLeakCheck()
                    .fails(7, "column must appear in GROUP BY clause or aggregate function");
        });
    }

    @Test
    public void testGroupByOrderByLocalColumnShadowsOuterReference() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE outer_t (id INT, k INT)");
            execute("CREATE TABLE inner_t (k INT, x INT)");
            final String query = """
                    SELECT * FROM outer_t o CROSS JOIN LATERAL (
                        SELECT count() n FROM inner_t i WHERE i.k = o.k GROUP BY i.x ORDER BY %s
                    ) d
                    """;
            final String qualified = query.formatted("i.k");
            assertQuery(qualified).noLeakCheck().fails(qualified.lastIndexOf("i.k"), "ORDER BY expressions must appear in select list. Invalid column: i.k");
            final String unqualified = query.formatted("k");
            assertQuery(unqualified).noLeakCheck().fails(unqualified.lastIndexOf("k"), "ORDER BY expressions must appear in select list. Invalid column: k");
            final String unknown = query.formatted("o.missing");
            assertQuery(unknown).noLeakCheck().fails(unknown.indexOf("o.missing"), "Invalid table name or alias");
            final String shadowedAlias = """
                    SELECT * FROM outer_t o CROSS JOIN LATERAL (
                        SELECT count() n FROM inner_t o GROUP BY o.x ORDER BY o.k
                    ) d
                    """;
            assertQuery(shadowedAlias).noLeakCheck().fails(shadowedAlias.indexOf("o.k"), "ORDER BY expressions must appear in select list. Invalid column: o.k");
        });
    }

    @Test
    public void testGroupByOrderByOuterExpressionLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE outer_t (id INT, k INT)");
            execute("INSERT INTO outer_t VALUES (-1, 1), (1, 1)");
            execute("CREATE TABLE inner_t (k INT, x INT)");
            execute("INSERT INTO inner_t VALUES (1, 10), (1, 10), (1, 20)");
            for (boolean isFullFat : new boolean[]{false, true}) {
                assertQuery("""
                        SELECT o.id, d.n FROM outer_t o CROSS JOIN LATERAL (
                            SELECT count() + 0 n FROM inner_t i WHERE i.k = o.k
                            GROUP BY i.x ORDER BY i.x * id LIMIT 1
                        ) d ORDER BY o.id
                        """)
                        .noLeakCheck()
                        .expectSize(isFullFat)
                        .fullFatJoins(isFullFat)
                        .returns("""
                                id\tn
                                -1\t1
                                1\t2
                                """);
            }
        });
    }

    @Test
    public void testGroupByOrderByOuterReference() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE outer_t (id INT, k INT)");
            execute("INSERT INTO outer_t VALUES (1, 1), (2, 2), (3, NULL), (4, 3), (5, 1)");
            execute("CREATE TABLE inner_t (k INT, x INT)");
            execute("INSERT INTO inner_t VALUES (1, 10), (1, 20), (2, NULL), (NULL, 30)");
            final String query = """
                    SELECT o.id, d.n FROM outer_t o CROSS JOIN LATERAL (
                        SELECT count() n FROM inner_t i WHERE i.k = o.k GROUP BY i.x ORDER BY %s
                    ) d ORDER BY o.id, d.n
                    """;
            final String expected = """
                    id\tn
                    1\t1
                    1\t1
                    2\t1
                    3\t1
                    5\t1
                    5\t1
                    """;
            for (boolean isFullFat : new boolean[]{false, true}) {
                assertQuery(query.formatted("o.k")).noLeakCheck().fullFatJoins(isFullFat).expectSize(isFullFat).returns(expected);
                assertQuery(query.formatted("o.k + 1")).noLeakCheck().fullFatJoins(isFullFat).expectSize(isFullFat).returns(expected);
            }
        });
    }

    @Test
    public void testGroupByOuterExpressions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE outer_t (id INT, k INT)");
            execute("INSERT INTO outer_t VALUES (1, 1), (2, 2), (3, NULL), (4, 3), (5, 1)");
            execute("CREATE TABLE inner_t (k INT, x INT)");
            execute("INSERT INTO inner_t VALUES (1, 10), (1, 20), (2, NULL), (NULL, 30)");

            for (boolean isFullFat : new boolean[]{false, true}) {
                assertQuery("""
                        SELECT o.id, d.v, d.next, d.mixed, d.outer_id, d.n
                        FROM outer_t o
                        LEFT JOIN LATERAL (
                            SELECT o.k v, o.k + 1 next, coalesce(i.x, o.k) mixed, id outer_id, count() n
                            FROM inner_t i
                            WHERE i.k = o.k
                            GROUP BY i.x
                            ORDER BY o.k
                        ) d ON true
                        ORDER BY o.id, d.mixed
                        """)
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .returns("""
                                id\tv\tnext\tmixed\touter_id\tn
                                1\t1\t2\t10\t1\t1
                                1\t1\t2\t20\t1\t1
                                2\t2\t3\t2\t2\t1
                                3\tnull\tnull\t30\t3\t1
                                4\tnull\tnull\tnull\tnull\tnull
                                5\t1\t2\t10\t5\t1
                                5\t1\t2\t20\t5\t1
                                """);
            }
        });
    }

    @Test
    public void testGroupByOuterReference() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE outer_t (id INT, k INT)");
            execute("INSERT INTO outer_t VALUES (1, 1), (2, 2), (3, NULL), (4, 3), (5, 1)");
            execute("CREATE TABLE inner_t (k INT, x INT)");
            execute("INSERT INTO inner_t VALUES (1, 10), (1, 20), (2, NULL), (NULL, 30)");

            for (boolean isFullFat : new boolean[]{false, true}) {
                assertQuery("""
                        SELECT o.id, d.v, d.n
                        FROM outer_t o
                        CROSS JOIN LATERAL (
                            SELECT o.k v, count() n
                            FROM inner_t i
                            WHERE i.k = o.k
                            GROUP BY i.x
                        ) d
                        ORDER BY o.id, d.v, d.n
                        """)
                        .noLeakCheck()
                        .expectSize(isFullFat)
                        .fullFatJoins(isFullFat)
                        .returns("""
                                id\tv\tn
                                1\t1\t1
                                1\t1\t1
                                2\t2\t1
                                3\tnull\t1
                                5\t1\t1
                                5\t1\t1
                                """);
            }
        });
    }

    @Test
    public void testGroupByOverJoinedCorrelatedSubQuery() throws Exception {
        // #7728
        assertMemoryLeak(() -> {
            createOuterKeyed("(1, 1, 1), (2, 2, 2)");
            createAKeyed("(10, 1, 1), (11, 2, 2)");
            createB("(20, 1, 1), (21, 2, 3)");
            assertQuery("""
                    SELECT o.id, l.g, l.c FROM o JOIN LATERAL (
                        SELECT a.x g, count(*) c
                        FROM a CROSS JOIN (SELECT id, k, x FROM b WHERE x = o.x) s
                        GROUP BY a.x
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tg\tc
                            1\t1\t1
                            1\t2\t1
                            """);
            assertQuery("""
                    SELECT o.id, l.g, l.c FROM o JOIN LATERAL (
                        SELECT a.x g, count(*) c
                        FROM a JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON s.k = a.k
                        GROUP BY a.x
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tg\tc
                            1\t1\t1
                            """);
            assertQuery("""
                    SELECT o.id, l.g, l.c FROM o JOIN LATERAL (
                        SELECT s.x g, count(*) c
                        FROM a CROSS JOIN (SELECT id, k, x FROM b WHERE x = o.x) s
                        GROUP BY s.x
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tg\tc
                            1\t1\t2
                            """);
            assertQuery("""
                    SELECT o.id, l.c FROM o JOIN LATERAL (
                        SELECT count(*) c FROM a CROSS JOIN (SELECT id, k, x FROM b WHERE x = o.x) s
                    ) l ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc
                            1\t2
                            2\t0
                            """);
        });
    }

    @Test
    public void testGroupByOverJoinWithMixedCorrelation() throws Exception {
        // #7694
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (ok INT, k INT, v INT)");
            execute("INSERT INTO o VALUES (1, 10, 1), (2, 20, 2), (3, 30, 2)");
            execute("CREATE TABLE t (ts TIMESTAMP, id INT, v INT) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:01', 1, 1), ('2024-01-01T00:00:03', 2, 2), ('2024-01-01T00:00:05', 3, 3)");
            execute("CREATE TABLE p (ts TIMESTAMP, id INT, k INT) TIMESTAMP(ts)");
            execute("INSERT INTO p VALUES ('2024-01-01T00:00:00', 1, 10), ('2024-01-01T00:00:02', 2, 20), ('2024-01-01T00:00:04', 2, 30), ('2024-01-01T00:00:06', 3, 10)");
            assertQuery("""
                    SELECT o.ok, s.id, s.c
                    FROM o JOIN LATERAL (
                        SELECT t.id, count(*) c
                        FROM t JOIN p ON t.id = p.id
                        WHERE p.k = o.k AND t.v <= o.v
                        GROUP BY t.id
                    ) s
                    ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            ok\tid\tc
                            1\t1\t1
                            2\t2\t1
                            3\t2\t1
                            """);
            assertQuery("""
                    SELECT o.ok, s.id, s.c
                    FROM o JOIN LATERAL (
                        SELECT t.id, count(*) c
                        FROM t LEFT JOIN p ON t.id = p.id
                        WHERE p.k = o.k AND t.v <= o.v
                        GROUP BY t.id
                    ) s
                    ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            ok\tid\tc
                            1\t1\t1
                            2\t2\t1
                            3\t2\t1
                            """);
        });
    }

    @Test
    public void testGroupByOverJoinWithNonEqualityCorrelation() throws Exception {
        // #7694
        assertMemoryLeak(() -> {
            createOuterKeyed("(1, 1, 1), (2, 2, 2)");
            createAKeyed("(10, 1, 1), (11, 2, 2)");
            createB("(20, 1, 1), (21, 2, 3)");
            assertQuery("""
                    SELECT o.id, l.g, l.c FROM o JOIN LATERAL (
                        SELECT a.k g, count(*) c FROM a LEFT JOIN b ON a.x = b.k WHERE a.x < o.x GROUP BY a.k
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tg\tc
                            2\t1\t1
                            """);
            assertQuery("""
                    SELECT o.id, l.g, l.c FROM o JOIN LATERAL (
                        SELECT a.k g, count(*) c FROM a JOIN b ON a.x = b.k WHERE a.x < o.x GROUP BY a.k
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tg\tc
                            2\t1\t1
                            """);
        });
    }

    @Test
    public void testHorizonJoinOverCorrelatedSlave() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            execute("INSERT INTO o VALUES (5, NULL)");
            execute("INSERT INTO e VALUES (9, NULL, 'a', '2024-01-01T08:00:00.000000Z')");
            execute("INSERT INTO q VALUES ('a', NULL, 50.0, '2024-01-01T07:30:00.000000Z')");
            final String keyedBody = "SELECT count() n, sum(q.bid) b FROM e HORIZON JOIN (SELECT * FROM q WHERE k = %1$s) q ON (s) LIST (0) AS h WHERE e.k = %1$s";
            assertOverCorrelatedHorizonSlave(
                    keyedBody,
                    """
                            id\tn\tb
                            1\t3\t10.0
                            2\t3\t40.0
                            3\t2\t30.0
                            4\t0\tnull
                            5\t1\t50.0
                            """
            );
            assertOverCorrelatedHorizonSlave(
                    "SELECT count() n, sum(q.bid) b FROM e HORIZON JOIN (SELECT * FROM q WHERE k = %1$s) q LIST (0) AS h WHERE e.k = %1$s",
                    """
                            id\tn\tb
                            1\t3\t50.0
                            2\t3\t60.0
                            3\t2\t60.0
                            4\t0\tnull
                            5\t1\t50.0
                            """
            );
            assertOverCorrelatedHorizonSlave(
                    "SELECT count() n, sum(q.bid) b, sum(r.bid) rb FROM e HORIZON JOIN (SELECT * FROM q WHERE k = %1$s) q ON (s) HORIZON JOIN q r ON (s) LIST (0) AS h WHERE e.k = %1$s",
                    """
                            id\tn\tb\trb
                            1\t3\t10.0\t30.0
                            2\t3\t40.0\t70.0
                            3\t2\t30.0\t50.0
                            4\t0\tnull\tnull
                            5\t1\t50.0\t50.0
                            """
            );
            final String groupedBody = "SELECT e.x, sum(q.bid) b FROM e HORIZON JOIN (q WHERE k = %1$s) q ON (s) LIST (0) AS h WHERE e.k = %1$s";
            final String groupedRows = """
                    1\t1\tnull
                    1\t2\tnull
                    1\t7\t10.0
                    2\t3\tnull
                    2\t4\t20.0
                    2\t8\t20.0
                    3\t5\t30.0
                    3\t6\tnull
                    """;
            assertOverCorrelatedHorizonSlave(groupedBody, "id\tx\tb\n" + groupedRows + "5\t9\t50.0\n");
            assertQuery("SELECT o.id, d.* FROM o LEFT JOIN LATERAL (" + groupedBody.formatted("o.k") + ") d ON true ORDER BY 1, 2")
                    .noLeakCheck()
                    .returns("id\tx\tb\n" + groupedRows + "4\tnull\tnull\n5\t9\t50.0\n");
            assertQuery("SELECT o.id, d.n, d.b FROM o JOIN LATERAL (" + keyedBody.formatted("o.k") + ") d")
                    .noLeakCheck()
                    .assertsLogicalPlan("""
                            Project
                              columns: [o.id, coalesce(d.n, 0) AS n, d.b]
                              Join
                                Master o
                                  Scan
                                    table: o
                                    columns: [id, k]
                                LEFT d
                                  keys: [d.__qdb_outer_ref__0_k = o.k]
                                  Project
                                    columns: [n, b, __qdb_outer_ref__0_k]
                                    Aggregate
                                      keys: [k AS __qdb_outer_ref__0_k]
                                      values: [count() AS n, sum(bid) AS b]
                                      HorizonJoin
                                        offsets: [0]
                                        Scan
                                          table: e
                                          columns: [k, s, ts]
                                        keys: [s = s, __qdb_outer_ref__0_k = k]
                                        Project
                                          columns: [s, bid, ts, k AS __qdb_outer_ref__0_k]
                                          Scan
                                            table: q
                                            columns: [s, bid, ts, k]
                            """);
        });
    }

    @Test
    public void testHorizonJoinOverCorrelatedSlaveNeedingDomain() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            final String unmappedByMaster = """
                    SELECT o.id, d.n FROM o JOIN LATERAL (
                        SELECT count() n FROM e HORIZON JOIN (SELECT * FROM q WHERE k = o.k) q ON (s) LIST (0) AS h
                    ) d
                    """;
            assertQuery(unmappedByMaster)
                    .noLeakCheck()
                    .fails(unmappedByMaster.indexOf("HORIZON"), "left-hand side of HORIZON JOIN can only be a table with an optional filter");
            final String nonEquality = """
                    SELECT o.id, d.n FROM o JOIN LATERAL (
                        SELECT count() n FROM e HORIZON JOIN (SELECT * FROM q WHERE k > o.k) q ON (s) LIST (0) AS h WHERE e.k = o.k
                    ) d
                    """;
            assertQuery(nonEquality)
                    .noLeakCheck()
                    .fails(nonEquality.indexOf("HORIZON"), "right-hand side of HORIZON JOIN can only be a table with an optional filter");
            execute("CREATE TABLE ol (id INT, k LONG)");
            final String typeMismatch = """
                    SELECT o.id, d.n FROM ol o JOIN LATERAL (
                        SELECT count() n FROM e HORIZON JOIN (SELECT * FROM q WHERE k = o.k) q ON (s) LIST (0) AS h WHERE e.x = o.id
                    ) d
                    """;
            assertQuery(typeMismatch)
                    .noLeakCheck()
                    .fails(typeMismatch.indexOf("HORIZON"), "right-hand side of HORIZON JOIN can only be a table with an optional filter");
        });
    }

    @Test
    public void testInnerJoinKeysSharingColumnNamesInBody() throws Exception {
        // #7716
        assertMemoryLeak(() -> {
            createOuterKeyed("(1, 1, 1), (2, 2, 2)");
            createAKeyed("(10, 1, 1), (11, 2, 2)");
            createB("(20, 1, 1), (21, 2, 3)");
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a JOIN b ON a.x = b.k AND b.x = o.k
                    ) l ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            """);
        });
    }

    @Test
    public void testInnerJoinOnReadingOuterColumnBeforeRightOrFullJoin() throws Exception {
        // #7723, #7737
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t JOIN xs x ON x.k = o.k FULL JOIN refunds r ON r.k = t.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t JOIN xs x ON x.k = o.k RIGHT JOIN refunds r ON r.k = t.x WHERE r.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            2\tnull\t101
                            """);
        });
    }

    @Test
    public void testJoinedCorrelatedSubQueryKeepsOnWithSecondOuterColumn() throws Exception {
        // #7725
        assertMemoryLeak(() -> {
            createOuterKeyed("(1, 1, 1)");
            createA("(10, 1)");
            createB("(20, 5, 1)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid, o.x ox
                        FROM a LEFT JOIN (SELECT id, k, x FROM b WHERE x = o.k) s ON a.x = s.k
                    ) l
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a JOIN (SELECT id, k, x FROM b WHERE x = o.k) s ON a.x = s.k
                        WHERE a.x = o.x
                    ) l
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\taid\tsid
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a LEFT JOIN (SELECT id, k, x FROM b WHERE x = o.k AND id > o.x) s ON a.x = s.k
                    ) l
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\tnull
                            """);
        });
    }

    @Test
    public void testJoinedTablesSharingColumnNameWithTwoCorrelatedConditions() throws Exception {
        // #7836
        assertMemoryLeak(() -> {
            createOuter("(1, 10), (2, 20)");
            createAKeyOnly("(1, 1), (2, 2)");
            execute("CREATE TABLE d (id INT)");
            execute("CREATE TABLE o2 (oid INT, ox INT)");
            execute("INSERT INTO d VALUES (1), (2)");
            execute("INSERT INTO o2 VALUES (1, 10), (2, 20)");
            assertQuery("""
                    SELECT o.id, t.aid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid FROM a JOIN d ON d.id = a.id WHERE a.id = o.id AND a.k <= o.x
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid
                            1\t1
                            2\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.aid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid FROM a JOIN d ON d.id = a.id WHERE a.id = o.id AND a.k != o.x
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid
                            1\t1
                            2\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.aid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid FROM a JOIN d ON d.id = a.id WHERE d.id = o.id AND a.k <= o.x
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid
                            1\t1
                            2\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.aid FROM o LEFT JOIN LATERAL (
                        SELECT a.id aid FROM a JOIN d ON d.id = a.id WHERE a.id = o.id AND a.k <= o.x
                    ) t ON true ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid
                            1\t1
                            2\t2
                            """);
            assertQuery("""
                    SELECT o2.oid, t.aid FROM o2 CROSS JOIN LATERAL (
                        SELECT a.id aid FROM a JOIN d ON d.id = a.id WHERE a.id = o2.oid AND a.k <= o2.ox
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            oid\taid
                            1\t1
                            2\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.aid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid FROM a JOIN a d ON d.id = a.id WHERE a.id = o.id AND a.k <= o.x
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid
                            1\t1
                            2\t2
                            """);
        });
    }

    @Test
    public void testJoinToSubQueryWithCorrelatedOn() throws Exception {
        // #7729
        assertMemoryLeak(() -> {
            createOuterKeyed("(1, 1, 1), (2, 2, 2)");
            createAKeyed("(10, 1, 1), (11, 2, 2)");
            createB("(20, 1, 1), (21, 2, 3)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a JOIN (SELECT id, k FROM b) s ON s.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            1\t11\t20
                            2\t10\t21
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a JOIN (SELECT id, k FROM b) s ON a.x = s.k AND s.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a JOIN (SELECT id, k FROM b) s ON a.x = s.k AND a.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a JOIN b ON a.x = b.k AND b.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a LEFT JOIN (SELECT id, k FROM b) s ON a.x = s.k AND s.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            1\t11\tnull
                            2\t10\tnull
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a CROSS JOIN (SELECT id, k FROM b) s WHERE a.x = s.k AND s.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            2\t11\t21
                            """);
        });
    }

    @Test
    public void testKeylessAggregateReadByFilterOrJoin() throws Exception {
        // #7803 section 2
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, 9)");
            execute("CREATE TABLE p (k INT, v INT)");
            execute("INSERT INTO p VALUES (0, 10)");
            execute("CREATE TABLE c (x INT)");
            execute("INSERT INTO c VALUES (1)");
            execute("CREATE TABLE s (k INT, v INT)");
            execute("INSERT INTO s VALUES (0, 500), (1, 501)");
            assertQuery("""
                    SELECT o.id, t.cnt FROM o CROSS JOIN LATERAL (
                        SELECT cnt FROM (SELECT count(*)::INT cnt FROM c WHERE x = o.x) WHERE cnt < 5
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcnt
                            1\t1
                            2\t0
                            """);
            assertQuery("""
                    SELECT o.id, t.cnt, t.sv FROM o CROSS JOIN LATERAL (
                        SELECT n1.cnt, n2.v sv
                        FROM (SELECT count(*)::INT cnt FROM c WHERE x = o.x) n1 JOIN s n2 ON n2.k = n1.cnt
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcnt\tsv
                            1\t1\t501
                            2\t0\t500
                            """);
            assertQuery("""
                    SELECT o.id, t.pv, t.cnt, t.sv FROM o CROSS JOIN LATERAL (
                        SELECT p.v pv, r.cnt, s.v sv FROM p
                        LEFT JOIN (SELECT count(*)::INT cnt FROM c WHERE x = o.x) r ON true
                        JOIN s ON s.k = r.cnt
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tpv\tcnt\tsv
                            1\t10\t1\t501
                            2\t10\t0\t500
                            """);
            assertQuery("""
                    SELECT o.id, t.pv, t.cnt FROM o CROSS JOIN LATERAL (
                        SELECT p.v pv, r.cnt FROM p
                        LEFT JOIN (SELECT count(*)::INT cnt FROM c WHERE x = o.x) r ON r.cnt = p.k
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tpv\tcnt
                            1\t10\tnull
                            2\t10\t0
                            """);
            assertQuery("""
                    SELECT o.id, t.pv, t.cnt, t.sv FROM o CROSS JOIN LATERAL (
                        SELECT p.v pv, r.cnt, s.v sv FROM p
                        LEFT JOIN (SELECT count(*)::INT cnt FROM c WHERE x = o.x) r ON true
                        LEFT JOIN s ON s.k = r.cnt
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tpv\tcnt\tsv
                            1\t10\t1\t501
                            2\t10\t0\t500
                            """);
            assertQuery("""
                    SELECT o.id, t.pv, t.cnt FROM o CROSS JOIN LATERAL (
                        SELECT p.v pv, r.cnt FROM p
                        JOIN (SELECT count(*)::INT cnt FROM c WHERE x = o.x) r ON r.cnt = p.k
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tpv\tcnt
                            2\t10\t0
                            """);
        });
    }

    @Test
    public void testLateralOnConditionKeptWithNonEqualityCorrelation() throws Exception {
        // #7803 section 1
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, x INT, y INT)");
            execute("INSERT INTO o VALUES (1, 1, 1, 0), (2, 2, 1, 0)");
            execute("CREATE TABLE p (k INT, x INT, y INT, v INT)");
            execute("INSERT INTO p VALUES (1, 1, 5, 10), (2, 1, 5, 20)");
            assertQuery("""
                    SELECT o.id, t.v FROM o JOIN LATERAL (
                        SELECT v, k jk FROM p WHERE x = o.x AND y > o.y
                    ) t ON t.jk = o.k
                    ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t10
                            2\t20
                            """);
            assertQuery("""
                    SELECT o.id, t.v FROM o JOIN LATERAL (
                        SELECT v, k jk FROM p WHERE x = o.x AND y > o.y
                    ) t ON t.jk = 1
                    ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t10
                            2\t10
                            """);
            assertQuery("""
                    SELECT o.id, t.v FROM o LEFT JOIN LATERAL (
                        SELECT v, k jk FROM p WHERE x = o.x AND y > o.y
                    ) t ON t.jk = o.k
                    ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t10
                            2\t20
                            """);
        });
    }

    @Test
    public void testLatestOnAboveOrBelowCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            assertQuery("SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x, s FROM e WHERE k = o.k LATEST ON ts PARTITION BY s) d ORDER BY o.id, d.x")
                    .noLeakCheck()
                    .returns("""
                            id\tx
                            1\t2
                            1\t7
                            2\t3
                            2\t8
                            3\t5
                            3\t6
                            """);
            final String latestPerSymbol = """
                    id\tx
                    1\t7
                    2\t8
                    """;
            assertQuery("SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM (SELECT x, k, ts FROM e LATEST ON ts PARTITION BY s) d WHERE d.k = o.k) d ORDER BY o.id, d.x")
                    .noLeakCheck()
                    .returns(latestPerSymbol);
            assertQuery("SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM (e LATEST ON ts PARTITION BY s) WHERE k = o.k) d ORDER BY o.id, d.x")
                    .noLeakCheck()
                    .returns(latestPerSymbol);
            assertQuery("SELECT o.id, d.x FROM o JOIN (SELECT x, k FROM e LATEST ON ts PARTITION BY s) d ON d.k = o.k ORDER BY o.id, d.x")
                    .noLeakCheck()
                    .returns(latestPerSymbol);
        });
    }

    @Test
    public void testLeftJoinedCorrelatedSubQueryAfterInnerJoinWithWhereEquality() throws Exception {
        // #7725
        assertMemoryLeak(() -> {
            createOuterKeyed("(401, 0, 2), (402, NULL, 1), (403, 3, NULL), (404, 0, 3), (405, NULL, 0), (406, 3, 0)");
            execute("CREATE TABLE t0 (id INT, k INT, x INT)");
            execute("CREATE TABLE t4 (id INT, k INT, x INT)");
            execute("INSERT INTO t0 VALUES (1, 1, 3), (2, 3, 3), (3, 0, NULL)");
            execute("INSERT INTO t4 VALUES (401, 0, 2), (402, NULL, 1), (403, 3, NULL), (404, 0, 3), (405, NULL, 0), (406, 3, 0)");
            assertQuery("""
                    SELECT o.id, l.i0, l.i1, l.i2 FROM o LEFT JOIN LATERAL (
                        SELECT b0.id i0, b1.id i1, b2.id i2
                        FROM t0 b0 JOIN t4 b1 ON b0.k = b1.x
                        LEFT JOIN (SELECT id, k, x FROM t4 WHERE x = o.k) b2 ON b1.x = b2.k
                        WHERE b1.x = o.x
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ti0\ti1\ti2
                            401\tnull\tnull\tnull
                            402\t1\t402\tnull
                            403\tnull\tnull\tnull
                            404\t2\t404\t406
                            405\t3\t405\tnull
                            405\t3\t406\tnull
                            406\t3\t405\t404
                            406\t3\t406\t404
                            """);
        });
    }

    @Test
    public void testLeftJoinedCorrelatedSubQueryWithWhereEqualityOnMaster() throws Exception {
        // #7725
        assertMemoryLeak(() -> {
            createOuterKeyed("(1, 1, 1)");
            createA("(10, 1)");
            createB("(20, 5, 1)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a LEFT JOIN (SELECT id, k, x FROM b WHERE x = o.k) s ON a.x = s.k
                        WHERE a.x = o.x
                    ) l
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o LEFT JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a LEFT JOIN (SELECT id, k, x FROM b WHERE x = o.k) s ON a.x = s.k
                        WHERE a.x = o.x
                    ) l ON true
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\taid\tsid
                            1\t10\tnull
                            """);
        });
    }

    @Test
    public void testLeftJoinNonKeyOnReadingOuterColumnAfterCrossJoin() throws Exception {
        // #7700
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (id INT)");
            createA("(10, 1), (11, 2)");
            execute("CREATE TABLE b (id INT, k INT)");
            execute("CREATE TABLE c (k INT)");
            execute("CREATE TABLE o (id INT, k INT)");
            execute("INSERT INTO p VALUES (1), (2)");
            execute("INSERT INTO b VALUES (1, 1), (2, 2)");
            execute("INSERT INTO c VALUES (1)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a CROSS JOIN c LEFT JOIN b ON a.x = b.k AND b.id > p.id
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pid\taid\tbid
                            1\t10\tnull
                            1\t11\t2
                            2\t10\tnull
                            2\t11\tnull
                            """);
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a JOIN c ON c.k = a.x LEFT JOIN b ON a.x = b.k AND b.id > p.id
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pid\taid\tbid
                            1\t10\tnull
                            2\t10\tnull
                            """);
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a CROSS JOIN c LEFT JOIN b ON a.x = b.k AND b.id = p.id + 1
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pid\taid\tbid
                            1\t10\tnull
                            1\t11\t2
                            2\t10\tnull
                            2\t11\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.pid, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                            SELECT a.id aid, b.id bid FROM a JOIN c ON c.k = o.k LEFT JOIN b ON a.x = b.k AND b.id > p.id
                        ) m
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tpid\taid\tbid
                            1\t1\t10\tnull
                            1\t1\t11\t2
                            1\t2\t10\tnull
                            1\t2\t11\tnull
                            """);
        });
    }

    @Test
    public void testLeftJoinOnReadingOuterColumnAndCrossJoinedTable() throws Exception {
        // #7700
        assertMemoryLeak(() -> {
            createOuterKeyed("(1, 1, 1), (2, 2, 2)");
            createAKeyed("(10, 1, 1), (11, 2, 2)");
            createB("(20, 1, 1), (21, 2, 3)");
            execute("CREATE TABLE c (id INT, k INT, x INT)");
            execute("INSERT INTO c VALUES (30, 1, 1), (31, 3, 2)");
            assertQuery("""
                    SELECT o.id, l.aid, l.bid, l.cid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid, c.id cid
                        FROM a CROSS JOIN b LEFT JOIN c ON b.x = c.x AND a.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid\tcid
                            1\t10\t20\t30
                            1\t10\t21\tnull
                            1\t11\t20\tnull
                            1\t11\t21\tnull
                            2\t10\t20\tnull
                            2\t10\t21\tnull
                            2\t11\t20\t30
                            2\t11\t21\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid, l.cid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid, c.id cid
                        FROM a JOIN b ON a.k = b.k LEFT JOIN c ON b.x = c.x AND a.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid\tcid
                            1\t10\t20\t30
                            1\t11\t21\tnull
                            2\t10\t20\tnull
                            2\t11\t21\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid, l.cid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid, c.id cid
                        FROM a, b LEFT JOIN c ON b.x = c.x AND a.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid\tcid
                            1\t10\t20\t30
                            1\t10\t21\tnull
                            1\t11\t20\tnull
                            1\t11\t21\tnull
                            2\t10\t20\tnull
                            2\t10\t21\tnull
                            2\t11\t20\t30
                            2\t11\t21\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid, l.cid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid, c.id cid
                        FROM a JOIN b ON b.id > a.id LEFT JOIN c ON b.x = c.x AND a.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid\tcid
                            1\t10\t20\t30
                            1\t10\t21\tnull
                            1\t11\t20\tnull
                            1\t11\t21\tnull
                            2\t10\t20\tnull
                            2\t10\t21\tnull
                            2\t11\t20\t30
                            2\t11\t21\tnull
                            """);
        });
    }

    @Test
    public void testLeftJoinOnReadingOuterColumnBeforeEquatedInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT)");
            execute("CREATE TABLE t (id INT, k INT)");
            createAKeyOnly("(100, 1), (101, 2), (102, NULL)");
            createB("(200, 1, 10), (201, 2, 11), (202, NULL, 10)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2), (3, NULL)");
            execute("INSERT INTO t VALUES (10, 1), (11, 2)");
            final String expected = """
                    id\ttid\taid\tbid
                    1\t10\t100\t200
                    2\t11\t101\t201
                    3\t10\t102\t202
                    """;
            assertQuery("""
                    SELECT o.id, l.tid, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT t.id tid, a.id aid, b.id bid FROM t LEFT JOIN a ON a.k = o.k JOIN b ON b.x = t.id WHERE b.k = o.k
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("""
                    SELECT o.id, l.tid, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT t.id tid, a.id aid, b.id bid FROM t LEFT JOIN a ON a.k = o.k
                        LEFT JOIN b ON b.x = t.id AND b.id > 0 WHERE b.k = o.k
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testLeftJoinOnWithTwoOuterColumnsEquatedToOneColumn() throws Exception {
        assertMemoryLeak(() -> {
            createOuterKeyed("(1, 1, 1), (2, 2, 3)");
            execute("CREATE TABLE t (id INT, k INT)");
            createAKeyOnly("(100, 1), (101, 2)");
            execute("INSERT INTO t VALUES (10, 1), (11, 2)");
            assertQuery("""
                    SELECT o.id, l.tid, l.aid FROM o JOIN LATERAL (
                        SELECT t.id tid, a.id aid FROM t LEFT JOIN a ON a.k = o.k AND a.k = o.x WHERE t.k = o.k AND t.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\taid
                            1\t10\t100
                            """);
        });
    }

    @Test
    public void testLiftedSelectionReadByJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (k INT, n INT)");
            execute("INSERT INTO m VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE s (k INT, v INT)");
            execute("INSERT INTO s VALUES (1, 1)");
            execute("CREATE TABLE q (x INT)");
            execute("INSERT INTO q VALUES (11), (20)");
            execute("CREATE TABLE q2 (x INT, k INT)");
            execute("INSERT INTO q2 VALUES (11, 1), (20, 2), (11, 2)");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (boolean isFullFat : new boolean[]{false, true}) {
                    compiler.setFullFatJoins(isFullFat);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.v FROM m
                            LEFT JOIN LATERAL (SELECT s.v + m.n v FROM s WHERE s.k = m.k) l ON l.v > 5 ORDER BY m.k
                            """, """
                            k\tv
                            1\t11
                            2\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.v FROM m
                            LEFT JOIN LATERAL (SELECT s.v + m.n v FROM s WHERE s.k = m.k) l ON l.v > 50 ORDER BY m.k
                            """, """
                            k\tv
                            1\tnull
                            2\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.v FROM m
                            INNER JOIN LATERAL (SELECT s.v + m.n v FROM s WHERE s.k = m.k) l ON l.v > 5 ORDER BY m.k
                            """, """
                            k\tv
                            1\t11
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.v, l.w FROM m
                            LEFT JOIN LATERAL (SELECT s.v + m.n v, s.v w FROM s WHERE s.k = m.k) l ON l.v > 5 AND l.w = 1 ORDER BY m.k
                            """, """
                            k\tv\tw
                            1\t11\t1
                            2\tnull\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.v FROM m
                            LEFT JOIN LATERAL (SELECT s.v + m.n v FROM s WHERE s.k = m.k) l ON true WHERE l.v > 5 ORDER BY m.k
                            """, """
                            k\tv
                            1\t11
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.v FROM m
                            LEFT JOIN LATERAL (SELECT coalesce(s.v, 0) + m.n v FROM s WHERE s.k = m.k) l ON true ORDER BY m.k
                            """, """
                            k\tv
                            1\t11
                            2\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.x FROM m
                            LEFT JOIN LATERAL (SELECT m.n + 1 x FROM s WHERE s.k = m.k) l ON true ORDER BY m.k
                            """, """
                            k\tx
                            1\t11
                            2\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.v, q.x FROM m
                            CROSS JOIN LATERAL (SELECT s.v + m.n v FROM s WHERE s.k = m.k) l
                            JOIN q ON q.x = l.v ORDER BY m.k
                            """, """
                            k\tv\tx
                            1\t11\t11
                            """, isFullFat, true);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.v, q.x FROM m
                            CROSS JOIN LATERAL (SELECT s.v + m.n v FROM s WHERE s.k = m.k) l
                            LEFT JOIN q ON q.x < l.v ORDER BY m.k
                            """, """
                            k\tv\tx
                            1\t11\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.v FROM m
                            LEFT JOIN LATERAL (SELECT s.v + m.n - 1 v FROM s WHERE s.k = m.k) l ON l.v = m.n ORDER BY m.k
                            """, """
                            k\tv
                            1\t10
                            2\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.v, q2.x FROM m
                            CROSS JOIN LATERAL (SELECT s.v + m.n v FROM s WHERE s.k = m.k) l
                            JOIN q2 ON q2.x = l.v AND q2.k = m.k ORDER BY m.k
                            """, """
                            k\tv\tx
                            1\t11\t11
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.v, q.x FROM m
                            CROSS JOIN LATERAL (SELECT coalesce(s.v + m.n, -1) v FROM s WHERE s.k = m.k) l
                            RIGHT JOIN q ON q.x = m.n + 1 ORDER BY q.x
                            """, """
                            k\tv\tx
                            1\t11\t11
                            null\tnull\t20
                            """);
                }
            }
            assertQuery("""
                    SELECT m.k, l.v FROM m
                    LEFT JOIN LATERAL (SELECT s.v + m.n v FROM s WHERE s.k = m.k) l ON l.v > 5
                    """)
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [m.k,l.__qdb_lifted_0_v+m.n]
                                Hash Left Outer Join Light
                                  condition: l.__qdb_outer_ref__0_k=m.k
                                  filter: 5<l.__qdb_lifted_0_v+m.n
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: m
                                    Hash
                                        SelectedRecord
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: s
                            """);
            assertQuery("""
                    SELECT m.k, l.v FROM m
                    LEFT JOIN LATERAL (SELECT s.v + m.n - 1 v FROM s WHERE s.k = m.k) l ON l.v = m.n
                    """)
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [m.k,case([l.__qdb_row_marker__1,l.__qdb_lifted_0_v+m.n-1,null])]
                                Hash Left Outer Join Light
                                  condition: l.__qdb_outer_ref__0_k=m.k
                                  filter: l.__qdb_lifted_0_v+m.n-1=m.n
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: m
                                    Hash
                                        VirtualRecord
                                          functions: [k,v,true]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: s
                            """);
        });
    }

    @Test
    public void testLimitAboveOrBelowCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            assertQuery("SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM t WHERE k = o.k ORDER BY x LIMIT 2) d ORDER BY o.id, d.x")
                    .noLeakCheck()
                    .returns("""
                            id\tx
                            1\t1
                            1\t2
                            2\t3
                            2\t4
                            3\t5
                            3\t6
                            """);
            final String firstThree = """
                    id\tx
                    1\t1
                    1\t2
                    2\t3
                    """;
            assertLimitBelowCorrelation("SELECT o.id, d.x FROM o JOIN (SELECT x, k FROM t ORDER BY x LIMIT 3) d ON d.k = o.k ORDER BY o.id, d.x", firstThree);
            assertLimitBelowCorrelation(
                    "SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM (SELECT x, k FROM t ORDER BY x LIMIT 3) d WHERE d.k = o.k) d ORDER BY o.id, d.x",
                    firstThree
            );
            assertLimitBelowCorrelation(
                    "SELECT o.id, d.x FROM o JOIN LATERAL (SELECT * FROM (SELECT x, k FROM t ORDER BY x LIMIT 3) d WHERE d.k = o.k) d ORDER BY o.id, d.x",
                    firstThree
            );
            assertLimitBelowCorrelation("""
                    WITH c AS (SELECT x, k FROM t ORDER BY x LIMIT 3)
                    SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM c WHERE c.k = o.k) d ORDER BY o.id, d.x
                    """, firstThree);
            assertLimitBelowCorrelation(
                    "SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM (SELECT x, k FROM t LIMIT 3) d WHERE d.k = o.k) d ORDER BY o.id, d.x",
                    firstThree
            );
            assertLimitBelowCorrelation(
                    "SELECT o.id, d.x FROM o JOIN LATERAL (((SELECT x, k FROM t ORDER BY x LIMIT 3) WHERE x > 0) WHERE k = o.k) d ORDER BY o.id, d.x",
                    firstThree
            );
            assertLimitBelowCorrelation("""
                    SELECT o.id, d.x FROM o JOIN LATERAL (
                        SELECT x FROM (SELECT x, k FROM (SELECT x, k FROM t ORDER BY x LIMIT 3) e WHERE e.x > 0) d WHERE d.k = o.k
                    ) d ORDER BY o.id, d.x
                    """, firstThree);
            assertQuery("""
                    SELECT o.id, d.x FROM o JOIN LATERAL (
                        SELECT a.x FROM (SELECT x, k FROM t ORDER BY x LIMIT 3) a JOIN t b ON a.x = b.x AND b.k = o.k
                    ) d ORDER BY o.id, d.x
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(firstThree);
            final String leftFirstThree = """
                    id\tx
                    1\t1
                    1\t2
                    2\t3
                    3\tnull
                    4\tnull
                    """;
            assertLimitBelowCorrelation("SELECT o.id, d.x FROM o LEFT JOIN (SELECT x, k FROM t ORDER BY x LIMIT 3) d ON d.k = o.k ORDER BY o.id, d.x", leftFirstThree);
            assertLimitBelowCorrelation(
                    "SELECT o.id, d.x FROM o LEFT JOIN LATERAL (SELECT x FROM (SELECT x, k FROM t ORDER BY x LIMIT 3) d WHERE d.k = o.k) d ON true ORDER BY o.id, d.x",
                    leftFirstThree
            );
            final String secondToFourth = """
                    id\tx
                    1\t2
                    2\t3
                    2\t4
                    """;
            assertLimitBelowCorrelation("SELECT o.id, d.x FROM o JOIN (SELECT x, k FROM t ORDER BY x LIMIT 1, 4) d ON d.k = o.k ORDER BY o.id, d.x", secondToFourth);
            assertLimitBelowCorrelation(
                    "SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM (SELECT x, k FROM t ORDER BY x LIMIT 1, 4) d WHERE d.k = o.k) d ORDER BY o.id, d.x",
                    secondToFourth
            );
            final String lastThree = """
                    id\tx
                    1\t7
                    2\t8
                    3\t6
                    """;
            assertLimitBelowCorrelation("SELECT o.id, d.x FROM o JOIN (SELECT x, k FROM t ORDER BY x DESC LIMIT 3) d ON d.k = o.k ORDER BY o.id, d.x", lastThree);
            assertLimitBelowCorrelation(
                    "SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM (SELECT x, k FROM t ORDER BY x DESC LIMIT 3) d WHERE d.k = o.k) d ORDER BY o.id, d.x",
                    lastThree
            );
            final String scalarFirstThree = """
                    id\tc\ts
                    1\t2\t3
                    2\t1\t3
                    3\t0\tnull
                    4\t0\tnull
                    """;
            assertQuery("SELECT o.id, count(d.x) c, sum(d.x) s FROM o LEFT JOIN (SELECT x, k FROM t ORDER BY x LIMIT 3) d ON d.k = o.k GROUP BY o.id ORDER BY o.id")
                    .noLeakCheck()
                    .expectSize()
                    .returns(scalarFirstThree);
            assertLimitBelowCorrelation(
                    "SELECT o.id, d.c, d.s FROM o JOIN LATERAL (SELECT count() c, sum(x) s FROM (SELECT x, k FROM t ORDER BY x LIMIT 3) d WHERE d.k = o.k) d ORDER BY o.id",
                    scalarFirstThree
            );
            assertLimitBelowCorrelation("""
                    SELECT o.id, d.c, d.s FROM o LEFT JOIN LATERAL (
                        SELECT count() c, sum(x) s FROM (SELECT x, k FROM t ORDER BY x LIMIT 3) d WHERE d.k = o.k
                    ) d ON true ORDER BY o.id
                    """, scalarFirstThree);
            assertLimitBelowCorrelation("""
                    SELECT o.id, d.x FROM o JOIN LATERAL (
                        SELECT x FROM (SELECT x, k FROM t ORDER BY x LIMIT 4) d WHERE d.k = o.k AND d.x > o.id
                    ) d ORDER BY o.id, d.x
                    """, """
                    id\tx
                    1\t2
                    2\t3
                    2\t4
                    """);
            assertQuery("""
                    SELECT o.id, d.x FROM o JOIN LATERAL (
                        SELECT x FROM (SELECT x, k FROM t ORDER BY x LIMIT 5) d WHERE d.k = o.k LIMIT 1
                    ) d ORDER BY o.id, d.x
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tx
                            1\t1
                            2\t3
                            3\t5
                            """);
        });
    }

    @Test
    public void testLimitBetweenCorrelatedPredicates() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            final String expected = """
                    id\tx
                    1\t2
                    2\t3
                    2\t4
                    3\t5
                    3\t6
                    """;
            assertQuery("""
                    SELECT 1 id, x FROM (SELECT x FROM (SELECT x, k FROM t WHERE x > 1 ORDER BY x LIMIT 3) WHERE k = 1)
                    UNION ALL SELECT 2, x FROM (SELECT x FROM (SELECT x, k FROM t WHERE x > 2 ORDER BY x LIMIT 3) WHERE k = 2)
                    UNION ALL SELECT 3, x FROM (SELECT x FROM (SELECT x, k FROM t WHERE x > 3 ORDER BY x LIMIT 3) WHERE k = 3)
                    UNION ALL SELECT 4, x FROM (SELECT x FROM (SELECT x, k FROM t WHERE x > 10 ORDER BY x LIMIT 3) WHERE k = 10)
                    ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("""
                    SELECT o.id, d.x FROM o JOIN LATERAL (
                        SELECT x FROM (SELECT x, k FROM t WHERE x > o.id ORDER BY x LIMIT 3) d WHERE d.k = o.k
                    ) d ORDER BY o.id, d.x
                    """)
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("""
                    SELECT o.id, d.x FROM o JOIN LATERAL (
                        SELECT x FROM (SELECT x, k FROM t WHERE x > o.k ORDER BY x LIMIT 3) d WHERE d.k = o.k
                    ) d ORDER BY o.id, d.x
                    """)
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("""
                    SELECT o.id, d.x FROM o JOIN LATERAL (
                        SELECT x FROM (SELECT x, k FROM t WHERE x > o.id ORDER BY x LIMIT 3) d WHERE d.k = o.k LIMIT 1
                    ) d ORDER BY o.id, d.x
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tx
                            1\t2
                            2\t3
                            3\t5
                            """);
        });
    }

    @Test
    public void testLimitBodySelectingCorrelatedColumnOrWildcard() throws Exception {
        // #7803 section 10
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, 2)");
            execute("CREATE TABLE b (bid INT, x INT)");
            execute("INSERT INTO b VALUES (20, 1), (21, 1), (22, 2)");
            assertQuery("""
                    SELECT o.id, t.bid, t.x FROM o CROSS JOIN LATERAL (
                        SELECT bid, x FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\tx
                            1\t20\t1
                            2\t22\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT * FROM b WHERE x = o.x LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tbid
                            1\t20
                            2\t22
                            """);
            assertQuery("""
                    SELECT o.id, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT * FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid
                            1\t20
                            2\t22
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.x1 FROM o CROSS JOIN LATERAL (
                        SELECT bid, x + 1 x1 FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\tx1
                            1\t20\t2
                            2\t22\t3
                            """);
            assertQuery("""
                    SELECT o.id, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT s.* FROM (SELECT bid FROM b WHERE x = o.x) s LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tbid
                            1\t20
                            2\t22
                            """);
            assertQuery("""
                    SELECT o.id, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT bid FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid
                            1\t20
                            2\t22
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.bx FROM o CROSS JOIN LATERAL (
                        SELECT bid, x bx FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\tbx
                            1\t20\t1
                            2\t22\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.ox FROM o CROSS JOIN LATERAL (
                        SELECT bid, x, o.x ox FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\tox
                            1\t20\t1
                            2\t22\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.x FROM o LEFT JOIN LATERAL (
                        SELECT bid, x FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ON true ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\tx
                            1\t20\t1
                            2\t22\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT b.* FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid
                            1\t20
                            2\t22
                            """);
        });
    }

    @Test
    public void testLimitOrderingInBody() throws Exception {
        // #7803 section 8
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, 2)");
            execute("CREATE TABLE c (v INT, x INT, w INT, g INT)");
            execute("INSERT INTO c VALUES (301, 1, 5, 1), (302, 1, 3, 1), (303, 1, 7, 2), (304, 2, 9, 1), (305, 2, 1, 2), (306, 2, 4, 2), (307, 1, 1, 2)");
            assertQuery("""
                    SELECT o.id, t.v, t.w FROM o CROSS JOIN LATERAL (
                        SELECT -v AS v, w FROM (SELECT v, w FROM c WHERE x = o.x) ORDER BY v LIMIT 2
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv\tw
                            1\t-307\t1
                            1\t-303\t7
                            2\t-306\t4
                            2\t-305\t1
                            """);
            assertQuery("""
                    SELECT o.id, t.v, t.w FROM o CROSS JOIN LATERAL (
                        SELECT w AS v, v AS w FROM (SELECT v, w FROM c WHERE x = o.x) ORDER BY v LIMIT 2
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv\tw
                            1\t1\t307
                            1\t3\t302
                            2\t1\t305
                            2\t4\t306
                            """);
            assertQuery("""
                    SELECT o.id, t.v, t.w FROM o CROSS JOIN LATERAL (
                        SELECT v, w FROM (
                            SELECT v, w FROM (SELECT v, w FROM c WHERE x = o.x) ORDER BY w LIMIT 2
                        ) LIMIT 1000
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tv\tw
                            1\t302\t3
                            1\t307\t1
                            2\t305\t1
                            2\t306\t4
                            """);
            assertQuery("""
                    SELECT o.id, t.g, t.w FROM o CROSS JOIN LATERAL (
                        SELECT g, w FROM (
                            SELECT g, first(w) w FROM (
                                SELECT g, w FROM (SELECT g, w FROM c WHERE x = o.x) ORDER BY w DESC
                            ) GROUP BY g
                        ) LIMIT 1000
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tg\tw
                            1\t1\t5
                            1\t2\t7
                            2\t1\t9
                            2\t2\t4
                            """);
            assertQuery("""
                    SELECT o.id, t.v, t.w, t.rn FROM o CROSS JOIN LATERAL (
                        SELECT v, w, rn FROM (
                            SELECT v, w, row_number() OVER () rn FROM (
                                SELECT v, w FROM (SELECT v, w FROM c WHERE x = o.x) ORDER BY w
                            )
                        ) LIMIT 1000
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tv\tw\trn
                            1\t301\t5\t3
                            1\t302\t3\t2
                            1\t303\t7\t4
                            1\t307\t1\t1
                            2\t304\t9\t3
                            2\t305\t1\t1
                            2\t306\t4\t2
                            """);
        });
    }

    @Test
    public void testLimitPerOuterRowOverRightJoin() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        ORDER BY r.id DESC LIMIT 1
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            2\tnull\t101
                            """);
        });
    }

    @Test
    public void testLimitRanksInOrderOfSortUnderFilter() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            assertQuery("SELECT o.id, d.s FROM o JOIN LATERAL (SELECT s FROM (SELECT k, sum(x) s FROM t GROUP BY k ORDER BY s) WHERE s > o.id * 3 LIMIT 1) d ORDER BY o.id, d.s")
                    .noLeakCheck()
                    .withPlanContaining("orderedFunctions: [[s] => [row_number()")
                    .returns("""
                            id\ts
                            1\t10
                            2\t10
                            3\t10
                            4\t15
                            """);
            assertQuery("SELECT o.id, d.s FROM o JOIN LATERAL (SELECT s FROM (SELECT k, sum(x) s FROM t GROUP BY k ORDER BY s DESC) WHERE s < o.id * 5 LIMIT 1) d ORDER BY o.id, d.s")
                    .noLeakCheck()
                    .withPlanContaining("orderedFunctions: [[s desc] => [row_number()")
                    .returns("""
                            id\ts
                            3\t11
                            4\t15
                            """);
            assertQuery("SELECT o.id, d.y FROM o JOIN LATERAL (SELECT y FROM (SELECT x * 2 y, k FROM t ORDER BY y) WHERE y > o.id * 2 LIMIT 2) d ORDER BY o.id, d.y")
                    .noLeakCheck()
                    .withPlanContaining("orderedFunctions: [[y] => [row_number()")
                    .returns("""
                            id\ty
                            1\t4
                            1\t6
                            2\t6
                            2\t8
                            3\t8
                            3\t10
                            4\t10
                            4\t12
                            """);
            assertQuery("""
                    SELECT o.id, d.ts, d.s FROM o JOIN LATERAL (
                        SELECT ts, s FROM (SELECT ts, sum(x) s FROM e SAMPLE BY 2h FILL(NULL)) WHERE s > o.id * 4 LIMIT 2
                    ) d ORDER BY o.id, d.ts
                    """)
                    .noLeakCheck()
                    .withPlanContaining("orderedFunctions: [[ts] => [row_number()")
                    .returns("""
                            id\tts\ts
                            1\t2024-01-01T02:00:00.000000Z\t7
                            1\t2024-01-01T04:00:00.000000Z\t11
                            2\t2024-01-01T04:00:00.000000Z\t11
                            2\t2024-01-01T06:00:00.000000Z\t15
                            3\t2024-01-01T06:00:00.000000Z\t15
                            """);
            assertQuery("""
                    SELECT o.id, d.ts, d.s FROM o LEFT JOIN LATERAL (
                        SELECT * FROM (SELECT ts, sum(x) s FROM e SAMPLE BY 2h FILL(NULL)) WHERE s > o.id * 4 LIMIT 1, 2
                    ) d ON true ORDER BY o.id, d.ts
                    """)
                    .noLeakCheck()
                    .withPlanContaining("orderedFunctions: [[ts] => [row_number()")
                    .returns("""
                            id\tts\ts
                            1\t2024-01-01T04:00:00.000000Z\t11
                            2\t2024-01-01T06:00:00.000000Z\t15
                            3\t\tnull
                            4\t\tnull
                            """);
            final String nextTwo = """
                    id\tx
                    1\t2
                    1\t3
                    2\t3
                    2\t4
                    3\t4
                    3\t5
                    4\t5
                    4\t6
                    """;
            assertQuery("""
                    SELECT 1 id, x FROM (SELECT x FROM (SELECT x, k FROM t ORDER BY x) WHERE x > 1 LIMIT 2)
                    UNION ALL SELECT 2, x FROM (SELECT x FROM (SELECT x, k FROM t ORDER BY x) WHERE x > 2 LIMIT 2)
                    UNION ALL SELECT 3, x FROM (SELECT x FROM (SELECT x, k FROM t ORDER BY x) WHERE x > 3 LIMIT 2)
                    UNION ALL SELECT 4, x FROM (SELECT x FROM (SELECT x, k FROM t ORDER BY x) WHERE x > 4 LIMIT 2)
                    ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns(nextTwo);
            assertQuery("SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM (SELECT x, k FROM t ORDER BY x) WHERE x > o.id LIMIT 2) d ORDER BY o.id, d.x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(nextTwo);
        });
    }

    @Test
    public void testLimitScalarBodyKeepsJoinCondition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (k INT, n INT)");
            execute("INSERT INTO m VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE s (k INT, v INT)");
            execute("INSERT INTO s VALUES (1, 1)");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (boolean isFullFat : new boolean[]{false, true}) {
                    compiler.setFullFatJoins(isFullFat);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.c FROM m
                            INNER JOIN LATERAL (SELECT count() c FROM s WHERE s.k = m.k LIMIT 1) l ON l.c > 0 ORDER BY m.k
                            """, """
                            k\tc
                            1\t1
                            """, isFullFat, true);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.c FROM m
                            INNER JOIN LATERAL (SELECT count() c FROM s WHERE s.k = m.k LIMIT 1) l ON l.c = 0 ORDER BY m.k
                            """, """
                            k\tc
                            2\t0
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.c FROM m
                            INNER JOIN LATERAL (SELECT count() c FROM s WHERE s.k = m.k LIMIT 0) l ON l.c > 0 ORDER BY m.k
                            """, """
                            k\tc
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.c FROM m
                            INNER JOIN LATERAL (SELECT count() c FROM s WHERE s.k = m.k LIMIT 1) l ON m.k > 1 ORDER BY m.k
                            """, """
                            k\tc
                            2\t0
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.c FROM m
                            INNER JOIN LATERAL (SELECT count() c FROM s WHERE s.k = m.k LIMIT 1) l ON l.c > 0 AND m.k > 0 ORDER BY m.k
                            """, """
                            k\tc
                            1\t1
                            """, isFullFat, true);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.c FROM m
                            INNER JOIN LATERAL (SELECT count() c FROM s WHERE s.k = m.k LIMIT 1) l ON true WHERE l.c > 0 ORDER BY m.k
                            """, """
                            k\tc
                            1\t1
                            """, isFullFat, true);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.s FROM m
                            INNER JOIN LATERAL (SELECT sum(v) s FROM s WHERE s.k = m.k LIMIT 1) l ON l.s > 0 ORDER BY m.k
                            """, """
                            k\ts
                            1\t1
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.s FROM m
                            INNER JOIN LATERAL (SELECT sum(v) s FROM s WHERE s.k = m.k LIMIT m.k - 1) l ON m.k > 0 ORDER BY m.k
                            """, """
                            k\ts
                            2\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.c FROM m
                            CROSS JOIN LATERAL (SELECT count() c FROM s WHERE s.k = m.k LIMIT 1) l WHERE l.c > 0 ORDER BY m.k
                            """, """
                            k\tc
                            1\t1
                            """, isFullFat, true);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.c FROM m
                            CROSS JOIN LATERAL (SELECT count() c FROM s WHERE s.k = m.k LIMIT 0) l ORDER BY m.k
                            """, """
                            k\tc
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.c FROM m
                            CROSS JOIN LATERAL (SELECT count() c FROM s WHERE s.k = m.k LIMIT 1) l WHERE m.k > 1 ORDER BY m.k
                            """, """
                            k\tc
                            2\t0
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.c FROM m
                            LEFT JOIN LATERAL (SELECT count() c FROM s WHERE s.k = m.k LIMIT 1) l ON l.c > 0 ORDER BY m.k
                            """, """
                            k\tc
                            1\t1
                            2\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.c FROM m
                            LEFT JOIN LATERAL (SELECT count() c FROM s WHERE s.k = m.k LIMIT 0) l ON l.c >= 0 ORDER BY m.k
                            """, """
                            k\tc
                            1\tnull
                            2\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT m.k, l.c FROM m
                            LEFT JOIN LATERAL (SELECT count() c FROM s WHERE s.k = m.k LIMIT 1) l ON m.k > 1 ORDER BY m.k
                            """, """
                            k\tc
                            1\tnull
                            2\t0
                            """);
                }
            }
        });
    }

    @Test
    public void testNegativeLimitAboveOrBelowCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            assertQuery("SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM t WHERE k = o.k ORDER BY x LIMIT -2) d ORDER BY o.id, d.x")
                    .noLeakCheck()
                    .returns("""
                            id\tx
                            1\t2
                            1\t7
                            2\t4
                            2\t8
                            3\t5
                            3\t6
                            """);
            final String lastThree = """
                    id\tx
                    1\t7
                    2\t8
                    3\t6
                    """;
            assertLimitBelowCorrelation("SELECT o.id, d.x FROM o JOIN (SELECT x, k FROM t ORDER BY x LIMIT -3) d ON d.k = o.k ORDER BY o.id, d.x", lastThree);
            assertLimitBelowCorrelation(
                    "SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM (SELECT x, k FROM t ORDER BY x LIMIT -3) d WHERE d.k = o.k) d ORDER BY o.id, d.x",
                    lastThree
            );
            assertLimitBelowCorrelation(
                    "SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM (SELECT x, k FROM t LIMIT -3) d WHERE d.k = o.k) d ORDER BY o.id, d.x",
                    lastThree
            );
            assertLimitBelowCorrelation("""
                    WITH c AS (SELECT x, k FROM t ORDER BY x LIMIT -3)
                    SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM c WHERE c.k = o.k) d ORDER BY o.id, d.x
                    """, lastThree);
            assertLimitBelowCorrelation(
                    "SELECT o.id, d.x FROM o LEFT JOIN LATERAL (SELECT x FROM (SELECT x, k FROM t ORDER BY x LIMIT -3) d WHERE d.k = o.k) d ON true ORDER BY o.id, d.x",
                    """
                            id\tx
                            1\t7
                            2\t8
                            3\t6
                            4\tnull
                            """
            );
            assertLimitBelowCorrelation("""
                    SELECT o.id, d.c, d.s FROM o LEFT JOIN LATERAL (
                        SELECT count() c, sum(x) s FROM (SELECT x, k FROM t ORDER BY x LIMIT -3) d WHERE d.k = o.k
                    ) d ON true ORDER BY o.id
                    """, """
                    id\tc\ts
                    1\t1\t7
                    2\t1\t8
                    3\t1\t6
                    4\t0\tnull
                    """);
        });
    }

    @Test
    public void testNestedLateralReadingCorrelatedSubQueryColumn() throws Exception {
        // #7803 section 15
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, 2)");
            createB("(20, 1, 1), (21, 2, 2)");
            execute("CREATE TABLE d (k INT)");
            execute("INSERT INTO d VALUES (1), (1), (2)");
            createAKeyOnly("(10, 1), (11, 2)");
            assertQuery("""
                    SELECT o.id, t.bid, t.c FROM o CROSS JOIN LATERAL (
                        SELECT q.id bid, w.c FROM (SELECT id, k FROM b WHERE x = o.x) q
                        CROSS JOIN LATERAL (SELECT count(*) c FROM d WHERE d.k = q.k) w
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tbid\tc
                            1\t20\t2
                            2\t21\t1
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.c FROM o CROSS JOIN LATERAL (
                        SELECT q.id bid, w.c FROM b q
                        CROSS JOIN LATERAL (SELECT count(*) c FROM d WHERE d.k = q.k) w
                        WHERE q.x = o.x
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tbid\tc
                            1\t20\t2
                            2\t21\t1
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.c FROM o CROSS JOIN LATERAL (
                        SELECT q.id bid, w.c FROM a
                        JOIN (SELECT id, k FROM b WHERE x = o.x) q ON q.k = a.k
                        CROSS JOIN LATERAL (SELECT count(*) c FROM d WHERE d.k = q.k) w
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tbid\tc
                            1\t20\t2
                            2\t21\t1
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.c FROM o CROSS JOIN LATERAL (
                        SELECT q.id bid, w.c FROM a
                        LEFT JOIN (SELECT id, k FROM b WHERE x = o.x) q ON q.k = a.k
                        CROSS JOIN LATERAL (SELECT count(*) c FROM d WHERE d.k = q.k) w
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tbid\tc
                            1\tnull\t0
                            1\t20\t2
                            2\tnull\t0
                            2\t21\t1
                            """);
        });
    }

    @Test
    public void testNestedLateralWithRightOrFullJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT)");
            execute("CREATE TABLE p (id INT)");
            createA("(10, 1), (11, 2)");
            execute("CREATE TABLE b (id INT, k INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("INSERT INTO p VALUES (1), (5)");
            execute("INSERT INTO b VALUES (5, 1), (6, 2)");
            assertQuery("""
                    SELECT o.id, l.pid, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                            SELECT a.id aid, b.id bid FROM a RIGHT JOIN b ON a.x = b.k AND b.id > p.id AND b.k = o.k
                        ) m
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tpid\taid\tbid
                            1\t1\tnull\t6
                            1\t1\t10\t5
                            1\t5\tnull\t5
                            1\t5\tnull\t6
                            2\t1\tnull\t5
                            2\t1\t11\t6
                            2\t5\tnull\t5
                            2\t5\t11\t6
                            """);
            assertQuery("""
                    SELECT o.id, l.pid, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                            SELECT a.id aid, b.id bid FROM a FULL JOIN b ON a.x = b.k AND b.id > p.id AND b.k = o.k
                        ) m
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tpid\taid\tbid
                            1\t1\tnull\t6
                            1\t1\t10\t5
                            1\t1\t11\tnull
                            1\t5\tnull\t5
                            1\t5\tnull\t6
                            1\t5\t10\tnull
                            1\t5\t11\tnull
                            2\t1\tnull\t5
                            2\t1\t10\tnull
                            2\t1\t11\t6
                            2\t5\tnull\t5
                            2\t5\t10\tnull
                            2\t5\t11\t6
                            """);
        });
    }

    @Test
    public void testNestedScalarAggregateCorrelatedWithBothLevels() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2), (3, 3)");
            execute("CREATE TABLE t (k INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10), (1, 11), (2, 20)");
            execute("CREATE TABLE r (id INT, k INT)");
            execute("INSERT INTO r VALUES (10, 1), (20, 9)");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (boolean isFullFat : new boolean[]{false, true}) {
                    compiler.setFullFatJoins(isFullFat);
                    assertQueryReturns(compiler, """
                            SELECT o.id, y.cnt FROM o CROSS JOIN LATERAL (
                                SELECT x.cnt FROM r r2
                                CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k AND t.k = r2.k) x
                            ) y ORDER BY o.id, y.cnt
                            """, """
                            id\tcnt
                            1\t0
                            1\t2
                            2\t0
                            2\t0
                            3\t0
                            3\t0
                            """, true, true);
                    assertQueryReturns(compiler, """
                            SELECT o.id, y.cnt FROM o CROSS JOIN LATERAL (
                                SELECT x.cnt FROM r r2
                                CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = r2.k AND t.v > o.id LIMIT 1) x
                            ) y ORDER BY o.id, y.cnt
                            """, """
                            id\tcnt
                            1\t0
                            1\t2
                            2\t0
                            2\t2
                            3\t0
                            3\t2
                            """, true, true);
                    assertQueryReturns(compiler, """
                            SELECT o.id, y.cnt FROM o CROSS JOIN LATERAL (
                                SELECT x.cnt FROM r r2
                                LEFT JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = r2.k AND t.v > o.id) x ON x.cnt > 0
                            ) y ORDER BY o.id, y.cnt
                            """, """
                            id\tcnt
                            1\tnull
                            1\t2
                            2\tnull
                            2\t2
                            3\tnull
                            3\t2
                            """, true, true);
                    assertQueryReturns(compiler, """
                            SELECT o.id, y.cnt FROM o CROSS JOIN LATERAL (
                                SELECT x.cnt FROM r r2
                                CROSS JOIN LATERAL (SELECT count(*) + o.id cnt FROM t WHERE t.k = r2.k) x
                            ) y ORDER BY o.id, y.cnt
                            """, """
                            id\tcnt
                            1\t1
                            1\t3
                            2\t2
                            2\t4
                            3\t3
                            3\t5
                            """, true, true);
                    assertQueryReturns(compiler, """
                            SELECT o.id, y.s FROM o CROSS JOIN LATERAL (
                                SELECT x.s FROM r r2
                                CROSS JOIN LATERAL (SELECT sum(v) s FROM t WHERE t.k = r2.k AND t.v > o.id + 9) x
                            ) y ORDER BY o.id, y.s
                            """, """
                            id\ts
                            1\tnull
                            1\t11
                            2\tnull
                            2\tnull
                            3\tnull
                            3\tnull
                            """, true, true);
                    assertQueryReturns(compiler, """
                            SELECT o.id, y.cnt FROM o LEFT JOIN LATERAL (
                                SELECT x.cnt FROM r r2
                                CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = r2.k AND t.v > o.id) x
                                WHERE r2.k = o.k
                            ) y ON true ORDER BY o.id, y.cnt
                            """, """
                            id\tcnt
                            1\t2
                            2\tnull
                            3\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT o.id, y.cnt FROM o CROSS JOIN LATERAL (
                                SELECT z.cnt FROM r r2 CROSS JOIN LATERAL (
                                    SELECT x.cnt FROM r r4
                                    CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = r4.k AND t.k = r2.k AND t.v > o.id) x
                                ) z
                            ) y ORDER BY o.id, y.cnt
                            """, """
                            id\tcnt
                            1\t0
                            1\t0
                            1\t0
                            1\t2
                            2\t0
                            2\t0
                            2\t0
                            2\t2
                            3\t0
                            3\t0
                            3\t0
                            3\t2
                            """, true, true);
                    assertQueryReturns(compiler, """
                            SELECT o.id, y.cnt, y.rid FROM o CROSS JOIN LATERAL (
                                SELECT x.cnt, r3.id rid FROM r r2
                                CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = r2.k AND t.v > o.id) x
                                RIGHT JOIN r r3 ON r3.k = r2.k + o.k - 1
                            ) y ORDER BY o.id, y.rid
                            """, """
                            id\tcnt\trid
                            1\t2\t10
                            1\t0\t20
                            2\tnull\t10
                            2\tnull\t20
                            3\tnull\t10
                            3\tnull\t20
                            """, true, true);
                }
            }
        });
    }

    @Test
    public void testNoInternalColumnsInWildcardOrCreateTableAs() throws Exception {
        // #7803 section 9
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, 2)");
            execute("CREATE TABLE b (bid INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO b VALUES (20, 1, '2024-01-01T00:00:00.000000Z'), (21, 1, '2024-01-01T00:00:01.000000Z'), (22, 2, '2024-01-01T00:00:02.000000Z')");
            assertQuery("""
                    SELECT o.id, t.* FROM o CROSS JOIN LATERAL (
                        SELECT * FROM (SELECT bid FROM b WHERE x = o.x) ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid
                            1\t20
                            2\t22
                            """);
            assertQuery("""
                    SELECT o.id, t.* FROM o CROSS JOIN LATERAL (
                        SELECT * FROM (SELECT bid, x bx, ts FROM b WHERE x = o.x) LATEST ON ts PARTITION BY bx
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\tbx\tts
                            1\t21\t1\t2024-01-01T00:00:01.000000Z
                            2\t22\t2\t2024-01-01T00:00:02.000000Z
                            """);
            assertQuery("""
                    SELECT o.id, t.* FROM o CROSS JOIN LATERAL (
                        SELECT bid FROM b WHERE x = o.x ORDER BY bid + o.x
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid
                            1\t20
                            1\t21
                            2\t22
                            """);
            assertQuery("""
                    SELECT * FROM o CROSS JOIN LATERAL (
                        SELECT * FROM (SELECT bid FROM b WHERE x = o.x) ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tx\tbid
                            1\t1\t20
                            2\t2\t22
                            """);
            execute("""
                    CREATE TABLE ct AS (
                        SELECT o.id, t.* FROM o CROSS JOIN LATERAL (
                            SELECT * FROM (SELECT bid FROM b WHERE x = o.x) ORDER BY bid LIMIT 1
                        ) t
                    )
                    """);
            assertQuery("SELECT \"column\", type FROM table_columns('ct')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\ttype
                            id\tINT
                            bid\tINT
                            """);
            assertQuery("SELECT * FROM ct ORDER BY 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tbid
                            1\t20
                            2\t22
                            """);
        });
    }

    @Test
    public void testNonEquiRightJoinWithCorrelatedWhere() throws Exception {
        // #7723
        assertMemoryLeak(() -> {
            createOuter("(1, 10), (2, 20)");
            execute("CREATE TABLE a (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            createB("(11, 1, 10), (12, 2, 20), (13, 3, 30)");
            execute("INSERT INTO a VALUES (1, 1, '2024-01-01T00:00:01.000000Z'), (2, 2, '2024-01-01T00:00:03.000000Z')");
            assertQuery("""
                    SELECT o.id, t.aid, t.cid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, c.id cid FROM a RIGHT JOIN b c ON c.k > a.k WHERE c.x != o.x
                    ) t ORDER BY o.id, t.cid, t.aid
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tcid
                            1\t1\t12
                            1\t1\t13
                            1\t2\t13
                            2\tnull\t11
                            2\t1\t13
                            2\t2\t13
                            """);
        });
    }

    @Test
    public void testNullRejectingFilterOnNonNullableColumnAfterRightJoin() throws Exception {
        // #7737
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, k INT)");
            execute("CREATE TABLE trades (id INT, x INT, s SHORT, f BOOLEAN)");
            execute("CREATE TABLE refunds (id INT, k INT)");
            execute("CREATE TABLE xs (k INT, v INT)");
            execute("INSERT INTO orders VALUES (1, 1), (2, 2)");
            execute("INSERT INTO trades VALUES (10, 1, 10, true)");
            execute("INSERT INTO refunds VALUES (100, 1), (101, 2)");
            execute("INSERT INTO xs VALUES (1, 100)");
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM (SELECT id, x, s FROM trades) t
                        RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        WHERE t.s < 11
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM (SELECT id, x, s FROM trades) t
                        RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        WHERE t.s IS NOT NULL
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM (SELECT id, x, f FROM trades) t
                        RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        WHERE t.f = false
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            2\tnull\t100
                            2\tnull\t101
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM (SELECT id, x, s FROM trades) t
                        JOIN xs x ON x.k = o.k
                        RIGHT JOIN refunds r ON r.k = t.x
                        WHERE t.s < 11
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t
                        RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        WHERE t.s < 11
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
        });
    }

    @Test
    public void testOrderByLimitOverJoinCorrelatedOnJoinedTable() throws Exception {
        // #7695
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (ok INT, k INT, v INT)");
            execute("INSERT INTO o VALUES (1, 10, 1), (2, 20, 2), (3, 30, 2)");
            execute("CREATE TABLE t (ts TIMESTAMP, id INT, v INT) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:01', 1, 1), ('2024-01-01T00:00:03', 2, 2), ('2024-01-01T00:00:05', 3, 3)");
            execute("CREATE TABLE p (ts TIMESTAMP, id INT, k INT) TIMESTAMP(ts)");
            execute("INSERT INTO p VALUES ('2024-01-01T00:00:00', 1, 10), ('2024-01-01T00:00:02', 2, 20), ('2024-01-01T00:00:04', 2, 30), ('2024-01-01T00:00:06', 3, 10)");
            assertQuery("""
                    SELECT o.ok, s.id, s.pk
                    FROM o JOIN LATERAL (
                        SELECT t.id, p.k pk
                        FROM t JOIN p ON t.id = p.id
                        WHERE p.k = o.k
                        ORDER BY t.id
                        LIMIT 1
                    ) s
                    ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            ok\tid\tpk
                            1\t1\t10
                            2\t2\t20
                            3\t2\t30
                            """);
            assertQuery("""
                    SELECT o.ok, s.id, s.pk
                    FROM o JOIN LATERAL (
                        SELECT t.id, p.k pk
                        FROM t LEFT JOIN p ON t.id = p.id
                        WHERE p.k = o.k
                        ORDER BY t.id
                        LIMIT 1
                    ) s
                    ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            ok\tid\tpk
                            1\t1\t10
                            2\t2\t20
                            3\t2\t30
                            """);
            assertQuery("""
                    SELECT o.ok, s.id, s.k
                    FROM o JOIN LATERAL (
                        SELECT t.id, p.k
                        FROM t ASOF JOIN p ON (id)
                        WHERE p.k = o.k
                        ORDER BY t.id
                        LIMIT 1
                    ) s
                    ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            ok\tid\tk
                            1\t1\t10
                            2\t2\t20
                            """);
        });
    }

    @Test
    public void testOtherCorrelatedConditionsKeptWhenEveryOuterColumnIsEquated() throws Exception {
        // #7731
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT, y INT)");
            createA("(10, 1), (5, 1), (3, 5), (4, 5)");
            execute("CREATE TABLE c (k INT)");
            execute("INSERT INTO o VALUES (1, 1, 1), (2, 5, 5)");
            execute("INSERT INTO c VALUES (1), (5)");
            assertQuery("""
                    SELECT o.id, l.aid FROM o JOIN LATERAL (
                        SELECT a.id aid FROM a WHERE a.x = o.x AND a.id > o.x
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid
                            1\t5
                            1\t10
                            """);
            assertQuery("""
                    SELECT o.id, l.aid FROM o JOIN LATERAL (
                        SELECT a.id aid FROM a WHERE a.x = o.x AND a.id = o.x
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid
                            """);
            assertQuery("""
                    SELECT o.id, l.aid FROM o JOIN LATERAL (
                        SELECT a.id aid FROM a JOIN c ON c.k = a.x AND o.x > 1 WHERE a.x = o.x
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid
                            2\t3
                            2\t4
                            """);
            assertQuery("""
                    SELECT o.id, l.aid FROM o JOIN LATERAL (
                        SELECT a.id aid FROM a WHERE a.x = o.x AND o.x > 1
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid
                            2\t3
                            2\t4
                            """);
            assertQuery("""
                    SELECT o.id, l.aid FROM o LEFT JOIN LATERAL (
                        SELECT a.id aid FROM a WHERE a.x = o.x AND a.id > o.x
                    ) l ON true ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid
                            1\t5
                            1\t10
                            2\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid FROM o JOIN LATERAL (
                        SELECT a.id aid FROM a WHERE a.x = o.x AND a.id > o.y
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid
                            1\t5
                            1\t10
                            """);
        });
    }

    @Test
    public void testOuterColumnNamedNoArgFunctionInExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, pi DOUBLE)");
            execute("INSERT INTO o VALUES (1, 10), (2, 20), (3, NULL)");
            assertQuery("""
                    SELECT o.id, d.val FROM o
                    CROSS JOIN LATERAL (SELECT pi+1 AS val FROM long_sequence(1)) d ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tval
                            1\t11.0
                            2\t21.0
                            3\tnull
                            """);
        });
    }

    @Test
    public void testOuterColumnNamedNoArgFunctionInFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, pi DOUBLE)");
            execute("INSERT INTO o VALUES (1, 10), (2, 20), (3, NULL)");
            assertQuery("""
                    SELECT o.id, d.x FROM o
                    CROSS JOIN LATERAL (SELECT x FROM long_sequence(1) WHERE pi>15) d ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tx
                            2\t1
                            """);
        });
    }

    @Test
    public void testOuterColumnNamedNoArgFunctionInProjection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, pi DOUBLE)");
            execute("INSERT INTO o VALUES (1, 10), (2, 20), (3, NULL)");
            assertQuery("""
                    SELECT o.id, d.val FROM o
                    CROSS JOIN LATERAL (SELECT pi AS val FROM long_sequence(1)) d ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tval
                            1\t10.0
                            2\t20.0
                            3\tnull
                            """);
        });
    }

    @Test
    public void testOuterColumnNamedNoArgFunctionResolutionControls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, pi DOUBLE)");
            execute("INSERT INTO o VALUES (1, 10), (2, 20), (3, NULL)");
            assertQuery("""
                    SELECT o.id, d.* FROM o CROSS JOIN LATERAL (
                        SELECT pi AS local_pi, pi+1 AS local_val, o.pi AS outer_pi, pi() AS function_pi
                        FROM (SELECT 100.0 AS pi)
                    ) d ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tlocal_pi\tlocal_val\touter_pi\tfunction_pi
                            1\t100.0\t101.0\t10.0\t3.141592653589793
                            2\t100.0\t101.0\t20.0\t3.141592653589793
                            3\t100.0\t101.0\tnull\t3.141592653589793
                            """);
            assertQuery("""
                    SELECT d.* FROM long_sequence(1) o
                    CROSS JOIN LATERAL (SELECT pi AS val, pi+1 AS incremented FROM long_sequence(1)) d
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            val\tincremented
                            3.141592653589793\t4.141592653589793
                            """);
            assertException(
                    "SELECT d.* FROM o CROSS JOIN LATERAL (SELECT missing_col FROM long_sequence(1)) d",
                    45,
                    "Invalid column: missing_col"
            );
        });
    }

    @Test
    public void testOuterColumnNamedNoArgFunctionUsesNearestScope() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, pi DOUBLE)");
            execute("INSERT INTO o VALUES (1, 10), (2, 20), (3, NULL)");
            assertQuery("""
                    SELECT o.id, d.* FROM o CROSS JOIN LATERAL (
                        SELECT n.* FROM (SELECT o.pi+100 AS pi) p
                        CROSS JOIN LATERAL (
                            SELECT pi AS val, pi+1 AS incremented, o.pi AS outer_pi FROM long_sequence(1)
                        ) n
                    ) d ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tval\tincremented\touter_pi
                            1\t110.0\t111.0\t10.0
                            2\t120.0\t121.0\t20.0
                            3\tnull\tnull\tnull
                            """);
        });
    }

    @Test
    public void testOuterJoinOnReadingOuterColumnAfterCrossJoin() throws Exception {
        // #7700
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (id INT)");
            createA("(10, 1), (11, 2)");
            execute("CREATE TABLE b (id INT, k INT)");
            execute("CREATE TABLE c (k INT)");
            execute("INSERT INTO p VALUES (1), (2)");
            execute("INSERT INTO b VALUES (1, 1), (2, 2)");
            execute("INSERT INTO c VALUES (1)");
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a CROSS JOIN c RIGHT JOIN b ON a.x = b.k AND b.id > p.id WHERE a.id > 0
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pid\taid\tbid
                            1\t11\t2
                            """);
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a CROSS JOIN c FULL JOIN b ON a.x = b.k AND b.id > p.id WHERE a.id > 0
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pid\taid\tbid
                            1\t10\tnull
                            1\t11\t2
                            2\t10\tnull
                            2\t11\tnull
                            """);
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a CROSS JOIN c RIGHT JOIN b ON a.x = b.k AND b.id > p.id
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pid\taid\tbid
                            1\tnull\t1
                            1\t11\t2
                            2\tnull\t1
                            2\tnull\t2
                            """);
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a CROSS JOIN c FULL JOIN b ON a.x = b.k AND b.id > p.id
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pid\taid\tbid
                            1\tnull\t1
                            1\t10\tnull
                            1\t11\t2
                            2\tnull\t1
                            2\tnull\t2
                            2\t10\tnull
                            2\t11\tnull
                            """);
        });
    }

    @Test
    public void testOuterNegativeLimitOverCorrelatedLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE g3 (k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO g3 VALUES
                    (1, '2024-01-01T00:00:01.000000Z'),
                    (2, '2024-01-01T00:00:02.000000Z'),
                    (3, '2024-01-01T00:00:03.000000Z'),
                    (1, '2024-01-01T00:00:04.000000Z'),
                    (null, '2024-01-01T00:00:05.000000Z'),
                    (2, '2024-01-01T00:00:06.000000Z')
                    """);
            execute("CREATE TABLE g1 (k INT, v INT)");
            execute("INSERT INTO g1 VALUES (1, 10), (1, 5), (2, 7), (null, 100)");
            execute("CREATE TABLE o (id INT)");
            execute("INSERT INTO o VALUES (1), (2)");
            final String lastTwo = """
                    k\tts\tv
                    null\t2024-01-01T00:00:05.000000Z\t100
                    2\t2024-01-01T00:00:06.000000Z\t7
                    """;
            assertQuery("SELECT * FROM (g3 q LEFT JOIN LATERAL (SELECT sum(v) v FROM g1 q2 WHERE q2.k = q.k) l ON true) ORDER BY ts LIMIT -4")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            k\tts\tv
                            3\t2024-01-01T00:00:03.000000Z\tnull
                            1\t2024-01-01T00:00:04.000000Z\t15
                            null\t2024-01-01T00:00:05.000000Z\t100
                            2\t2024-01-01T00:00:06.000000Z\t7
                            """);
            assertQuery("""
                    SELECT * FROM (
                        SELECT * FROM (g3 q LEFT JOIN LATERAL (SELECT sum(v) v FROM g1 q2 WHERE q2.k = q.k) l ON true) ORDER BY ts LIMIT -4
                    ) ORDER BY ts LIMIT -2
                    """)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(lastTwo);
            assertQuery("SELECT q.k, q.ts, l.v FROM g3 q JOIN LATERAL (SELECT sum(v) v FROM g1 q2 WHERE q2.k = q.k) l LIMIT -3")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            k\tts\tv
                            1\t2024-01-01T00:00:04.000000Z\t15
                            null\t2024-01-01T00:00:05.000000Z\t100
                            2\t2024-01-01T00:00:06.000000Z\t7
                            """);
            assertQuery("""
                    WITH x AS (SELECT q.k, q.ts, l.v FROM g3 q JOIN LATERAL (SELECT sum(v) v FROM g1 q2 WHERE q2.k = q.k) l)
                    SELECT * FROM x ORDER BY ts LIMIT -2
                    """)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(lastTwo);
            assertQuery("SELECT q.k, q.ts, l.v FROM g3 q LEFT JOIN LATERAL (SELECT sum(v) v FROM g1 q2 WHERE q2.k = q.k) l ON true ORDER BY q.ts LIMIT 1, -1")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            k\tts\tv
                            2\t2024-01-01T00:00:02.000000Z\t7
                            3\t2024-01-01T00:00:03.000000Z\tnull
                            1\t2024-01-01T00:00:04.000000Z\t15
                            null\t2024-01-01T00:00:05.000000Z\t100
                            """);
            assertQuery("""
                    SELECT o.id, x.k, x.v FROM o JOIN LATERAL (
                        SELECT q.k, l.v FROM g3 q JOIN LATERAL (SELECT sum(v) v FROM g1 q2 WHERE q2.k = q.k) l ORDER BY q.ts LIMIT -2
                    ) x ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tk\tv
                            1\tnull\t100
                            1\t2\t7
                            2\tnull\t100
                            2\t2\t7
                            """);
            final String lastTwoPerOuterRow = """
                    id\tk\tv
                    1\tnull\t15
                    1\t2\t15
                    2\tnull\t7
                    2\t2\t7
                    """;
            assertQuery("""
                    SELECT o.id, x.k, x.v FROM o JOIN LATERAL (
                        SELECT q.k, l.v FROM g3 q JOIN LATERAL (SELECT sum(v) v FROM g1 q2 WHERE q2.k = o.id) l ORDER BY q.ts LIMIT -2
                    ) x ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns(lastTwoPerOuterRow);
            assertQuery("""
                    SELECT * FROM (
                        SELECT 1 id, k, v FROM (SELECT q.k, l.v FROM g3 q JOIN LATERAL (SELECT sum(v) v FROM g1 q2 WHERE q2.k = 1) l ORDER BY q.ts LIMIT -2)
                        UNION ALL
                        SELECT 2 id, k, v FROM (SELECT q.k, l.v FROM g3 q JOIN LATERAL (SELECT sum(v) v FROM g1 q2 WHERE q2.k = 2) l ORDER BY q.ts LIMIT -2)
                    ) ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(lastTwoPerOuterRow);
        });
    }

    @Test
    public void testOuterOnlyFilterOverInnerJoinBody() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (k INT)");
            execute("CREATE TABLE a (k INT)");
            execute("CREATE TABLE b (k INT)");
            execute("INSERT INTO o VALUES (1), (2)");
            execute("INSERT INTO a VALUES (10)");
            execute("INSERT INTO b VALUES (10), (20)");
            assertQuery("SELECT o.k, s.k FROM o CROSS JOIN LATERAL (SELECT a.k FROM a JOIN b ON a.k = b.k WHERE o.k <> 2) s ORDER BY o.k")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\tk1
                            1\t10
                            """);
        });
    }

    @Test
    public void testOuterTimestampComparedWithTimestampNs() throws Exception {
        // #7803 section 12
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, ts TIMESTAMP)");
            execute("INSERT INTO o VALUES (1, '2024-01-01T00:00:01.000000Z'), (2, '2024-01-01T00:00:02.000000Z')");
            execute("CREATE TABLE n (v INT, tn TIMESTAMP_NS)");
            execute("INSERT INTO n VALUES (10, '2024-01-01T00:00:01.000000000Z'), (20, '2024-01-01T00:00:02.000000000Z')");
            assertQuery("""
                    SELECT o.id, t.ots, t.ol, t.ts1, t.v FROM o CROSS JOIN LATERAL (
                        SELECT o.ts ots, o.ts::LONG ol, o.ts + 1 ts1, v FROM n WHERE tn = o.ts
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tots\tol\tts1\tv
                            1\t2024-01-01T00:00:01.000000Z\t1704067201000000\t2024-01-01T00:00:01.000001Z\t10
                            2\t2024-01-01T00:00:02.000000Z\t1704067202000000\t2024-01-01T00:00:02.000001Z\t20
                            """);
        });
    }

    @Test
    public void testPositionalOrderByInLimitBody() throws Exception {
        // #7803 section 17
        assertMemoryLeak(() -> {
            assertQuery("""
                    SELECT * FROM (SELECT x v FROM long_sequence(3)) t1
                    CROSS JOIN LATERAL (
                        SELECT v FROM (SELECT x v FROM long_sequence(3)) t
                        WHERE t.v <= t1.v ORDER BY 1 DESC LIMIT 1
                    ) t2 ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            v\tv1
                            1\t1
                            2\t2
                            3\t3
                            """);
            assertQuery("""
                    SELECT * FROM (SELECT x v FROM long_sequence(3)) t1
                    CROSS JOIN LATERAL (
                        SELECT v FROM (SELECT x v FROM long_sequence(3)) t
                        WHERE t.v <= t1.v ORDER BY v DESC LIMIT 1
                    ) t2 ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            v\tv1
                            1\t1
                            2\t2
                            3\t3
                            """);
            assertQuery("""
                    SELECT * FROM (SELECT x v FROM long_sequence(3)) t1
                    CROSS JOIN LATERAL (
                        SELECT v, v * 10 w FROM (SELECT x v FROM long_sequence(3)) t
                        WHERE t.v <= t1.v ORDER BY 2 DESC, 1 LIMIT 1
                    ) t2 ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            v\tv1\tw
                            1\t1\t10
                            2\t2\t20
                            3\t3\t30
                            """);
            assertQuery("""
                    SELECT * FROM (SELECT x v FROM long_sequence(3)) t1
                    LEFT JOIN LATERAL (
                        SELECT v FROM (SELECT x v FROM long_sequence(3)) t
                        WHERE t.v <= t1.v ORDER BY 1 DESC LIMIT 1
                    ) t2 ON true ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            v\tv1
                            1\t1
                            2\t2
                            3\t3
                            """);
        });
    }

    @Test
    public void testPositionalOrderByInUnionBranch() throws Exception {
        // #7785
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (sym SYMBOL, s SYMBOL, l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO k VALUES ('a', 'a', 1, '2024-01-01T00:00:00.000000Z'), ('b', 'b', 2, '2024-01-01T12:00:00.000000Z'), ('a', 'a', 3, '2024-01-02T00:00:00.000000Z')");
            assertQuery("""
                    SELECT k.sym, z.l
                    FROM k
                    CROSS JOIN LATERAL (
                        SELECT l FROM k k2 WHERE k2.sym = k.sym
                        UNION ALL
                        (SELECT l FROM k k3 WHERE k3.sym = k.sym ORDER BY 1 DESC LIMIT 1)
                    ) z
                    ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            sym\tl
                            a\t1
                            a\t1
                            a\t3
                            a\t3
                            a\t3
                            a\t3
                            b\t2
                            b\t2
                            """);
            assertQuery("""
                    SELECT * FROM (
                        SELECT 'x' sym, 0L l
                        UNION ALL
                        SELECT k.sym, z.l
                        FROM k
                        CROSS JOIN LATERAL (
                            SELECT l FROM k k2 WHERE k2.sym = k.sym
                            UNION ALL
                            (SELECT l FROM k k3 WHERE k3.sym = k.sym ORDER BY 1 DESC LIMIT 1)
                        ) z
                    ) ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            sym\tl
                            a\t1
                            a\t1
                            a\t3
                            a\t3
                            a\t3
                            a\t3
                            b\t2
                            b\t2
                            x\t0
                            """);
        });
    }

    @Test
    public void testPrefixDomainJoinsAtFirstCorrelatedStep() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (id INT)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT)");
            execute("CREATE TABLE c (k INT)");
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a CROSS JOIN c FULL JOIN b ON a.x = b.k AND b.id > p.id
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .assertsLogicalPlan("""
                            Sort
                              keys: [pid, aid, bid]
                              Project
                                columns: [p.id AS pid, m.aid, m.bid]
                                Join
                                  Master p
                                    Scan
                                      table: p
                                      columns: [id]
                                  INNER m
                                    keys: [m.__qdb_outer_ref__0_id = p.id]
                                    Project
                                      columns: [id AS aid, id1 AS bid, __qdb_outer_ref__0_id]
                                      Project
                                        columns: [a.id, b.id AS id1, case(__qdb_outer_ref__0.__qdb_outer_ref__marker_0 = null, b.__qdb_outer_ref__0_id, __qdb_outer_ref__0.__qdb_outer_ref__0_id) AS __qdb_outer_ref__0_id]
                                        Join
                                          Master a
                                            Scan
                                              table: a
                                              columns: [id, x]
                                          CROSS c
                                            Scan
                                              table: c
                                              columns: []
                                          CROSS __qdb_outer_ref__0
                                            Aggregate
                                              keys: [id AS __qdb_outer_ref__0_id]
                                              values: [count() AS __qdb_outer_ref__marker_0]
                                              Scan
                                                table: p
                                                columns: [id]
                                          FULL b
                                            keys: [b.k = a.x, b.__qdb_outer_ref__0_id = __qdb_outer_ref__0.__qdb_outer_ref__0_id]
                                            on: b.id > b.__qdb_outer_ref__0_id
                                            Join
                                              Master
                                                Scan
                                                  table: b
                                                  columns: [id, k]
                                              CROSS __qdb_outer_ref__0_1
                                                Aggregate
                                                  keys: [id AS __qdb_outer_ref__0_id]
                                                  values: []
                                                  Scan
                                                    table: p
                                                    columns: [id]
                            """);
        });
    }

    @Test
    public void testProjectAboveOrBelowCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            assertQuery("SELECT o.id, d.v FROM o JOIN LATERAL (SELECT x * 10 + o.id v FROM t WHERE k = o.k) d ORDER BY o.id, d.v")
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t11
                            1\t21
                            1\t71
                            2\t32
                            2\t42
                            2\t82
                            3\t53
                            3\t63
                            """);
            assertQuery("SELECT o.id, d.v FROM o JOIN LATERAL (SELECT v FROM (SELECT x * 10 v, k FROM t) d WHERE d.k = o.k) d ORDER BY o.id, d.v")
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t10
                            1\t20
                            1\t70
                            2\t30
                            2\t40
                            2\t80
                            3\t50
                            3\t60
                            """);
        });
    }

    @Test
    public void testRenamedProjectionAliasChain() throws Exception {
        allowFunctionMemoization();
        assertMemoryLeak(() -> {
            assertQuery("""
                    SELECT m.b, l.a, l.b AS lb, l.c FROM (SELECT 100 AS b) m
                    CROSS JOIN LATERAL (
                        SELECT x AS a, a AS b, b+1 AS c FROM (SELECT 1 AS x)
                    ) l
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            b\ta\tlb\tc
                            100\t1\t1\t2
                            """);
            execute("CREATE TABLE alias_values (x LONG)");
            execute("INSERT INTO alias_values VALUES (1), (2), (NULL)");
            assertQuery("""
                    SELECT m.b, l.a, l.b AS lb, l.c, l.d FROM (SELECT 100 AS b, 200 AS c) m
                    CROSS JOIN LATERAL (
                        SELECT x AS a, a AS b, b AS c, c+1 AS d FROM alias_values
                    ) l ORDER BY l.a
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            b\ta\tlb\tc\td
                            100\tnull\tnull\tnull\tnull
                            100\t1\t1\t1\t2
                            100\t2\t2\t2\t3
                            """);
            assertQuery("SELECT x AS a, a AS b, b AS c, c AS d, d+1 AS e FROM alias_values ORDER BY a")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tc\td\te
                            null\tnull\tnull\tnull\tnull
                            1\t1\t1\t1\t2
                            2\t2\t2\t2\t3
                            """);
        });
    }

    @Test
    public void testRenamedProjectionAliasesPreserveSourcePrecedence() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE alias_values (x LONG, b LONG)");
            execute("INSERT INTO alias_values VALUES (1, 10), (2, NULL), (NULL, 30)");
            assertQuery("""
                    SELECT m.b, l.a, l.b AS lb, l.c, l.d FROM (SELECT 100 AS b) m
                    CROSS JOIN LATERAL (
                        SELECT x AS a, a AS b, b+1 AS c, m.b+1 AS d FROM alias_values
                    ) l ORDER BY l.a
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            b\ta\tlb\tc\td
                            100\tnull\tnull\t31\t101
                            100\t1\t1\t11\t101
                            100\t2\t2\tnull\t101
                            """);
        });
    }

    @Test
    public void testRenamedProjectionAliasesPreserveSourceQualification() throws Exception {
        assertMemoryLeak(() -> {
            assertException("SELECT s.x AS a,a AS b,s.b+1 AS c FROM long_sequence(2) s", 23, "Invalid column: s.b");
            assertException(
                    "SELECT o.b,l.* FROM (SELECT 100 AS b) o CROSS JOIN LATERAL " +
                            "(SELECT s.x AS a,a AS b,s.b+1 AS c FROM long_sequence(2) s) l",
                    83,
                    "Invalid column: s.b"
            );
            assertQuery("SELECT s.x AS a, a AS b, b+s.x AS c FROM long_sequence(2) s")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tc
                            1\t1\t2
                            2\t2\t4
                            """);
            assertQuery("""
                    SELECT s.x AS a, a AS b, b+s.x AS c, row_number() OVER () AS rn
                    FROM long_sequence(2) s ORDER BY a
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tc\trn
                            1\t1\t2\t1
                            2\t2\t4\t2
                            """);
        });
    }

    @Test
    public void testRightAndFullJoinInBodyMatchedRows() throws Exception {
        // #7803
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, 2)");
            createAKeyOnly("(10, 1), (11, 2)");
            createB("(20, 1, 1), (21, 2, 2)");
            assertQuery("""
                    SELECT o.id, t.aid, t.cid, t.sid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, c.id cid, s.id sid FROM a
                        FULL JOIN b c ON c.k = a.k
                        LEFT JOIN (SELECT id, k FROM b WHERE x != o.x) s ON s.k = a.k
                    ) t ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tcid\tsid
                            1\t10\t20\tnull
                            1\t11\t21\t21
                            2\t10\t20\t20
                            2\t11\t21\tnull
                            """);
            assertQuery("""
                    SELECT o.id, t.aid, t.cid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, c.id cid FROM a
                        FULL JOIN (SELECT id, k FROM b WHERE x = o.x) c ON c.k = a.k
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tcid
                            1\t10\t20
                            1\t11\tnull
                            2\t10\tnull
                            2\t11\t21
                            """);
        });
    }

    @Test
    public void testRightJoinBodyInUnionAllBranch() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        UNION ALL
                        SELECT -1, x.k FROM xs x WHERE x.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            1\t-1\t1
                            1\t10\t100
                            2\tnull\t100
                            2\tnull\t101
                            """);
        });
    }

    @Test
    public void testRightJoinedCorrelatedSubQueryWithNullOuterKey() throws Exception {
        // #7726
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, NULL)");
            createA("(10, 1)");
            createB("(20, 5, 1)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k
                        WHERE a.x = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.id = s.id
                        WHERE a.x = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            """);
            execute("INSERT INTO b VALUES (21, 1, 1), (22, 7, NULL)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k
                        WHERE a.x = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t21
                            2\tnull\t22
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.id = s.id
                        WHERE a.x = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            2\tnull\t22
                            """);
        });
    }

    @Test
    public void testRightJoinOnReadingOuterColumn() throws Exception {
        // #7723
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.c FROM orders o JOIN LATERAL (
                        SELECT count(*) c FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                    ) l ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc
                            1\t2
                            2\t2
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND t.x = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.rid FROM orders o JOIN LATERAL (
                        SELECT r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id IS NULL
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\trid
                            1\t101
                            2\t100
                            2\t101
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON r.k = o.k AND t.id > 0
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            1\t10\t100
                            2\tnull\t100
                            2\t10\t101
                            """);
        });
    }

    @Test
    public void testRightJoinOnReadingTwoOuterColumns() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            execute("CREATE TABLE o (id INT, k INT, lim INT)");
            execute("INSERT INTO o VALUES (1, 1, 5), (2, 1, 50)");
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k AND t.id > o.lim
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
        });
    }

    @Test
    public void testRightJoinPlanKeepsOnAndKeysSlaveDomain() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid
                        FROM (SELECT id, x FROM trades WHERE x = o.k) t RIGHT JOIN refunds r ON t.x = r.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .assertsLogicalPlan("""
                            Sort
                              keys: [id, tid, rid]
                              Project
                                columns: [o.id, l.tid, l.rid]
                                Join
                                  Master o
                                    Scan
                                      table: orders
                                      columns: [id, k]
                                  INNER l
                                    keys: [l.__qdb_outer_ref__0_k = o.k]
                                    Project
                                      columns: [t.id AS tid, r.id AS rid, r.__qdb_outer_ref__0_k]
                                      Join
                                        Master t
                                          Project
                                            columns: [id, x, x AS __qdb_outer_ref__0_k]
                                            Scan
                                              table: trades
                                              columns: [id, x]
                                        RIGHT r
                                          keys: [r.k = t.x, r.__qdb_outer_ref__0_k = t.__qdb_outer_ref__0_k]
                                          Join
                                            Master
                                              Scan
                                                table: refunds
                                                columns: [id, k]
                                            CROSS __qdb_outer_ref__0
                                              Aggregate
                                                keys: [k AS __qdb_outer_ref__0_k]
                                                values: []
                                                Scan
                                                  table: orders
                                                  columns: [k]
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid, l.xk FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid, x.k xk
                        FROM trades t FULL JOIN refunds r ON r.k = t.x
                        LEFT JOIN (SELECT k, v FROM xs WHERE k = o.k) x ON x.v > 0
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .assertsLogicalPlan("""
                            Sort
                              keys: [id, tid, rid]
                              Project
                                columns: [o.id, l.tid, l.rid, l.xk]
                                Join
                                  Master o
                                    Scan
                                      table: orders
                                      columns: [id, k]
                                  INNER l
                                    keys: [l.__qdb_outer_ref__0_k = o.k]
                                    Project
                                      columns: [t.id AS tid, r.id AS rid, x.k AS xk, __qdb_outer_ref__0.__qdb_outer_ref__0_k]
                                      Join
                                        Master t
                                          Scan
                                            table: trades
                                            columns: [id, x]
                                        FULL r
                                          keys: [r.k = t.x]
                                          Scan
                                            table: refunds
                                            columns: [id, k]
                                        CROSS __qdb_outer_ref__0
                                          Aggregate
                                            keys: [k AS __qdb_outer_ref__0_k]
                                            values: []
                                            Scan
                                              table: orders
                                              columns: [k]
                                        LEFT x
                                          keys: [x.__qdb_outer_ref__0_k = __qdb_outer_ref__0.__qdb_outer_ref__0_k]
                                          on: x.v > 0
                                          Project
                                            columns: [k, v, k AS __qdb_outer_ref__0_k]
                                            Scan
                                              table: xs
                                              columns: [k, v]
                            """);
        });
    }

    @Test
    public void testRightJoinToSubQueryWithCorrelatedOn() throws Exception {
        // #7729
        assertMemoryLeak(() -> {
            createOuterKeyed("(1, 1, 1), (2, 2, 2)");
            createAKeyed("(10, 1, 1), (11, 2, 2)");
            createB("(20, 1, 1), (21, 2, 3)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k FROM b) s ON a.x = s.k AND s.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\tnull\t21
                            1\t10\t20
                            2\tnull\t20
                            2\t11\t21
                            """);
        });
    }

    @Test
    public void testRightOrFullJoinWithNullOuterValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, k INT)");
            execute("CREATE TABLE trades (id INT, x INT)");
            execute("CREATE TABLE refunds (id INT, k INT)");
            execute("INSERT INTO orders VALUES (1, 1), (2, 2), (3, NULL)");
            execute("INSERT INTO trades VALUES (10, 1), (11, NULL)");
            execute("INSERT INTO refunds VALUES (100, 1), (101, 2), (102, NULL)");
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            1\tnull\t102
                            1\t10\t100
                            2\tnull\t100
                            2\tnull\t101
                            2\tnull\t102
                            3\tnull\t100
                            3\tnull\t101
                            3\t11\t102
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t FULL JOIN refunds r ON t.x = r.k AND r.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            1\tnull\t102
                            1\t10\t100
                            1\t11\tnull
                            2\tnull\t100
                            2\tnull\t101
                            2\tnull\t102
                            2\t10\tnull
                            2\t11\tnull
                            3\tnull\t100
                            3\tnull\t101
                            3\t10\tnull
                            3\t11\t102
                            """);
            execute("CREATE TABLE od (id INT, d DOUBLE, s SYMBOL)");
            execute("CREATE TABLE td (id INT, d DOUBLE)");
            execute("CREATE TABLE rd (id INT, s SYMBOL, d DOUBLE)");
            execute("INSERT INTO od VALUES (1, 1.5, 'a'), (2, NULL, NULL)");
            execute("INSERT INTO td VALUES (10, 1.5), (11, NULL)");
            execute("INSERT INTO rd VALUES (100, 'a', 1.5), (101, NULL, NULL), (102, 'b', 2.5)");
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM od o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM td t RIGHT JOIN rd r ON t.d = r.d AND r.s = o.s AND r.d = o.d
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            1\tnull\t102
                            1\t10\t100
                            2\tnull\t100
                            2\tnull\t102
                            2\t11\t101
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM od o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM td t FULL JOIN rd r ON t.d = r.d AND r.s = o.s AND r.d = o.d
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            1\tnull\t102
                            1\t10\t100
                            1\t11\tnull
                            2\tnull\t100
                            2\tnull\t102
                            2\t10\tnull
                            2\t11\t101
                            """);
        });
    }

    @Test
    public void testSameNamedOuterColumnsOfTwoOuterTables() throws Exception {
        // #7803 section 6
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o1 (id INT, x INT)");
            execute("INSERT INTO o1 VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE o2 (id INT, x INT)");
            execute("INSERT INTO o2 VALUES (1, 5), (2, 6)");
            createAKeyOnly("(10, 1), (11, 2)");
            execute("CREATE TABLE b (id INT, k INT, x INT, y INT)");
            execute("INSERT INTO b VALUES (20, 1, 1, 5), (21, 2, 2, 6)");
            assertQuery("""
                    SELECT p.id, t.aid, t.rid FROM o1 p JOIN o2 q ON q.id = p.id CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id rid FROM a
                        LEFT JOIN (SELECT id, k FROM b WHERE x = p.x AND y = q.x) r ON r.k = a.k
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\trid
                            1\t10\t20
                            1\t11\tnull
                            2\t10\tnull
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT p.id, t.aid, t.rid FROM o1 p JOIN o2 q ON q.id = p.id CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id rid FROM a
                        JOIN (SELECT id, k FROM b WHERE x = p.x AND y = q.x) r ON r.k = a.k
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\trid
                            1\t10\t20
                            2\t11\t21
                            """);
        });
    }

    @Test
    public void testSampleByAboveOrBelowCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            final String expected = """
                    id\tts\ta
                    1\t2024-01-01T00:00:00.000000Z\t1.5
                    1\t2024-01-01T02:00:00.000000Z\t3.3333333333333335
                    1\t2024-01-01T04:00:00.000000Z\t5.166666666666667
                    1\t2024-01-01T06:00:00.000000Z\t7.0
                    2\t2024-01-01T00:00:00.000000Z\t1.25
                    2\t2024-01-01T02:00:00.000000Z\t3.5
                    2\t2024-01-01T04:00:00.000000Z\t5.75
                    2\t2024-01-01T06:00:00.000000Z\t8.0
                    3\t2024-01-01T00:00:00.000000Z\tnull
                    3\t2024-01-01T02:00:00.000000Z\tnull
                    3\t2024-01-01T04:00:00.000000Z\t5.5
                    3\t2024-01-01T06:00:00.000000Z\tnull
                    """;
            assertQuery("SELECT o.id, d.ts, d.a FROM o JOIN LATERAL (SELECT ts, avg(x) a FROM e WHERE k = o.k SAMPLE BY 2h FILL(LINEAR)) d ORDER BY o.id, d.ts")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("""
                    SELECT o.id, d.ts, d.a FROM o JOIN LATERAL (
                        SELECT ts, a FROM (SELECT ts, k, avg(x) a FROM e SAMPLE BY 2h FILL(LINEAR)) d WHERE d.k = o.k
                    ) d ORDER BY o.id, d.ts
                    """)
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testSampleByFillOuterCopiedIntoDomain() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sb (sym SYMBOL INDEX, s STRING, i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO sb VALUES
                        ('a', 'x', 1, '2024-01-01T00:10:00.000000Z'),
                        ('b', 'q', 2, '2024-01-01T00:20:00.000000Z'),
                        ('a', 'y', 3, '2024-01-01T03:10:00.000000Z')
                    """);
            assertQuery("""
                    SELECT o.ts, o.f, l.c
                    FROM (SELECT ts, first(s) f, last(i) li FROM sb WHERE sym = 'a' SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION) o
                    JOIN LATERAL (SELECT count() c FROM sb x WHERE x.i <= o.li) l
                    ORDER BY o.ts
                    """)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tf\tc
                            2024-01-01T00:10:00.000000Z\tx\t1
                            2024-01-01T01:10:00.000000Z\tx\t1
                            2024-01-01T02:10:00.000000Z\tx\t1
                            2024-01-01T03:10:00.000000Z\ty\t3
                            """);
        });
    }

    @Test
    public void testScalarCountBeforeRightJoinInLeftLateral() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.rid, l.n FROM orders o LEFT JOIN LATERAL (
                        SELECT r.id rid, c.n FROM trades t
                        CROSS JOIN (SELECT count(*) n FROM xs WHERE k = o.k) c
                        RIGHT JOIN refunds r ON t.x = r.k
                    ) l ON true ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\trid\tn
                            1\t100\t1
                            1\t101\tnull
                            2\t100\t0
                            2\t101\tnull
                            """);
        });
    }

    @Test
    public void testScalarCountGuardedByNullableDerivedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, ts TIMESTAMP)");
            execute("""
                    INSERT INTO o VALUES
                        (1, 1, NULL),
                        (2, 2, NULL),
                        (3, 3, '2024-01-01T00:00:03.000000Z'),
                        (4, 4, '2024-01-01T00:00:04.000000Z')
                    """);
            execute("CREATE TABLE t (k INT)");
            execute("INSERT INTO t VALUES (1), (1), (3)");
            execute("CREATE TABLE r (id INT, k INT)");
            execute("INSERT INTO r VALUES (10, 1), (20, 2), (50, 5)");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (boolean isFullFat : new boolean[]{false, true}) {
                    compiler.setFullFatJoins(isFullFat);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, r.id rid FROM (SELECT id, k, ts FROM o ORDER BY ts) o TIMESTAMP(ts)
                            CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x
                            FULL JOIN r ON r.k = o.k ORDER BY o.id, rid
                            """, """
                            id\tcnt\trid
                            null\tnull\t50
                            1\t2\t10
                            2\t0\t20
                            3\t1\tnull
                            4\t0\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, r.id rid FROM (SELECT id, k, ts FROM o ORDER BY ts) o TIMESTAMP(ts)
                            CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x
                            RIGHT JOIN r ON r.k = o.k ORDER BY o.id, rid
                            """, """
                            id\tcnt\trid
                            null\tnull\t50
                            1\t2\t10
                            2\t0\t20
                            """);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, r.id rid FROM (SELECT id, k, ts FROM o ORDER BY ts) o TIMESTAMP(ts)
                            CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x
                            LEFT JOIN r ON r.k = o.k ORDER BY o.id, rid
                            """, """
                            id\tcnt\trid
                            1\t2\t10
                            2\t0\t20
                            3\t1\tnull
                            4\t0\tnull
                            """);
                }
            }
        });
    }

    @Test
    public void testScalarCountGuardedByOuterDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO o VALUES (1, 1, '2024-01-01T00:00:01.000000Z'), (2, 2, '2024-01-01T00:00:02.000000Z'), (3, 3, '2024-01-01T00:00:03.000000Z')");
            execute("CREATE TABLE t (k INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10), (1, 11), (2, 20)");
            execute("CREATE TABLE r (id INT, k INT)");
            execute("INSERT INTO r VALUES (10, 1), (20, 9)");
            final String sql = """
                    SELECT o.id, x.cnt, r.id rid FROM o
                    CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x
                    FULL JOIN r ON r.k = o.k ORDER BY o.id, rid
                    """;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (boolean isFullFat : new boolean[]{false, true}) {
                    compiler.setFullFatJoins(isFullFat);
                    assertQueryReturns(compiler, sql, """
                            id\tcnt\trid
                            null\tnull\t20
                            1\t2\t10
                            2\t1\tnull
                            3\t0\tnull
                            """);
                }
            }
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlanNotContaining("__qdb_row_marker__", "true]");
        });
    }

    @Test
    public void testScalarCountGuardedByOverriddenNativeTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, ts TIMESTAMP, native_ts TIMESTAMP) TIMESTAMP(native_ts)");
            execute("""
                    INSERT INTO o VALUES
                        (1, 1, NULL, '2024-01-01T00:00:01.000000Z'),
                        (2, 2, NULL, '2024-01-01T00:00:02.000000Z')
                    """);
            execute("CREATE TABLE t (k INT)");
            execute("INSERT INTO t VALUES (1), (1)");
            execute("CREATE TABLE r (id INT, k INT)");
            execute("INSERT INTO r VALUES (10, 1), (20, 2), (30, 3)");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (boolean isFullFat : new boolean[]{false, true}) {
                    compiler.setFullFatJoins(isFullFat);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, r.id rid FROM o TIMESTAMP(ts)
                            CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x
                            FULL JOIN r ON r.k = o.k ORDER BY o.id, rid
                            """, """
                            id\tcnt\trid
                            null\tnull\t30
                            1\t2\t10
                            2\t0\t20
                            """);
                }
            }
        });
    }

    @Test
    public void testScalarCountMarksOuterRowsOfRightJoinedPrefix() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2), (3, 3)");
            execute("CREATE TABLE t (k INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10), (1, 11), (2, 20)");
            execute("CREATE TABLE r (id INT, k INT)");
            execute("INSERT INTO r VALUES (10, 1), (20, 9)");
            execute("CREATE TABLE r2 (id INT, k INT)");
            execute("INSERT INTO r2 VALUES (100, 1), (200, 7)");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (boolean isFullFat : new boolean[]{false, true}) {
                    compiler.setFullFatJoins(isFullFat);
                    assertQueryReturns(compiler, """
                            SELECT o.id, r2.id r2id, x.cnt, r.id rid FROM o
                            RIGHT JOIN r2 ON r2.k = o.k
                            CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = r2.k) x
                            FULL JOIN r ON r.k = r2.k ORDER BY r2id, rid
                            """, """
                            id\tr2id\tcnt\trid
                            null\tnull\tnull\t20
                            1\t100\t2\t10
                            null\t200\t0\tnull
                            """);
                }
            }
            assertQuery("""
                    SELECT o.id, x.cnt, r.id rid FROM o
                    CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x
                    RIGHT JOIN r ON r.k = o.k
                    """)
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [o.id,case([o.__qdb_row_marker__0,coalesce(x.cnt,0),null]),r.id]
                                Hash Right Outer Join Light
                                  condition: r.k=o.k
                                    Hash Left Outer Join Light
                                      condition: x.__qdb_outer_ref__0_k=o.k
                                        VirtualRecord
                                          functions: [id,k,true]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: o
                                        Hash
                                            SelectedRecord
                                                GroupBy vectorized: true workers: 1
                                                  keys: [k]
                                                  values: [count(*)]
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: t
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: r
                            """);
        });
    }

    @Test
    public void testScalarCountNullExtendedByLaterOuterJoin() throws Exception {
        // #7875
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2), (3, 3)");
            execute("CREATE TABLE t (k INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10), (1, 11), (2, 20)");
            execute("CREATE TABLE r (id INT, k INT)");
            execute("INSERT INTO r VALUES (10, 1), (20, 9)");
            final String rightJoined = """
                    id\tcnt\trid
                    1\t2\t10
                    null\tnull\t20
                    """;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (boolean isFullFat : new boolean[]{false, true}) {
                    compiler.setFullFatJoins(isFullFat);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, r.id rid FROM o
                            CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x
                            RIGHT JOIN r ON r.k = o.k ORDER BY rid
                            """, rightJoined);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, r.id rid FROM o
                            CROSS JOIN LATERAL (SELECT count(v) cnt FROM t WHERE t.k = o.k) x
                            RIGHT JOIN r ON r.k = o.k ORDER BY rid
                            """, rightJoined);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, r.id rid FROM o
                            JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x ON true
                            RIGHT JOIN r ON r.k = o.k ORDER BY rid
                            """, rightJoined);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, r.id rid FROM o
                            LEFT JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x ON true
                            RIGHT JOIN r ON r.k = o.k ORDER BY rid
                            """, rightJoined);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, x.s, r.id rid FROM o
                            CROSS JOIN LATERAL (SELECT count(*) cnt, sum(v) s FROM t WHERE t.k = o.k) x
                            RIGHT JOIN r ON r.k = o.k ORDER BY rid
                            """, """
                            id\tcnt\ts\trid
                            1\t2\t21\t10
                            null\tnull\tnull\t20
                            """);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, r.id rid FROM o
                            CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x
                            RIGHT JOIN r ON r.k = o.k WHERE x.cnt IS NULL
                            """, """
                            id\tcnt\trid
                            null\tnull\t20
                            """, false, false);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, r.id rid FROM o
                            CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x
                            FULL JOIN r ON r.k = o.k ORDER BY o.id, rid
                            """, """
                            id\tcnt\trid
                            null\tnull\t20
                            1\t2\t10
                            2\t1\tnull
                            3\t0\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.c2, r.id rid FROM o
                            CROSS JOIN LATERAL (SELECT count(*) + 2 c2 FROM t WHERE t.k = o.k) x
                            FULL JOIN r ON r.k = o.k ORDER BY o.id, rid
                            """, """
                            id\tc2\trid
                            null\tnull\t20
                            1\t4\t10
                            2\t3\tnull
                            3\t2\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, r.id rid FROM o
                            CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x
                            RIGHT JOIN r ON r.k < o.k ORDER BY rid, o.id
                            """, """
                            id\tcnt\trid
                            2\t1\t10
                            3\t0\t10
                            null\tnull\t20
                            """);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, y.cnt, r.id rid FROM o
                            CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x
                            CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k + 1) y
                            FULL JOIN r ON r.k = o.k ORDER BY o.id, rid
                            """, """
                            id\tcnt\tcnt1\trid
                            null\tnull\tnull\t20
                            1\t2\t1\t10
                            2\t1\t0\tnull
                            3\t0\t0\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, r.id rid FROM o
                            LEFT JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x ON x.cnt = 0
                            FULL JOIN r ON r.k = o.k ORDER BY o.id, rid
                            """, """
                            id\tcnt\trid
                            null\tnull\t20
                            1\tnull\t10
                            2\tnull\tnull
                            3\t0\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, r.id rid FROM o
                            CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k LIMIT 0) x
                            FULL JOIN r ON r.k = o.k ORDER BY rid
                            """, """
                            id\tcnt\trid
                            null\tnull\t10
                            null\tnull\t20
                            """);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.s, x.five, r.id rid FROM o
                            CROSS JOIN LATERAL (SELECT sum(v) s, 5 five FROM t WHERE t.k = o.k) x
                            FULL JOIN r ON r.k = o.k ORDER BY o.id, rid
                            """, """
                            id\ts\tfive\trid
                            null\tnull\tnull\t20
                            1\t21\t5\t10
                            2\t20\t5\tnull
                            3\tnull\t5\tnull
                            """);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt, y.cnt, r.id rid FROM o
                            CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x
                            RIGHT JOIN r ON r.k = o.k
                            CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = r.k) y ORDER BY rid
                            """, """
                            id\tcnt\tcnt1\trid
                            1\t2\t2\t10
                            null\tnull\t0\t20
                            """);
                    assertQueryReturns(compiler, """
                            SELECT o.id, y.cnt, y.rid FROM o CROSS JOIN LATERAL (
                                SELECT x.cnt, r3.id rid FROM r r2
                                CROSS JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = r2.k) x
                                RIGHT JOIN r r3 ON r3.k = r2.k + o.k - 1
                            ) y ORDER BY o.id, y.rid
                            """, """
                            id\tcnt\trid
                            1\t2\t10
                            1\t0\t20
                            2\tnull\t10
                            2\tnull\t20
                            3\tnull\t10
                            3\tnull\t20
                            """, true, true);
                    assertQueryReturns(compiler, """
                            SELECT o.id, x.cnt FROM o
                            LEFT JOIN LATERAL (SELECT count(*) cnt FROM t WHERE t.k = o.k) x ON x.cnt = 0 ORDER BY o.id
                            """, """
                            id\tcnt
                            1\tnull
                            2\tnull
                            3\t0
                            """);
                }
            }
        });
    }

    @Test
    public void testSharedDomainReadByMoreThanOneConsumer() throws Exception {
        // #7803 section 3
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, 2)");
            createA("(10, 1), (11, 2)");
            execute("CREATE TABLE b (id INT, k INT, x INT, y INT)");
            execute("INSERT INTO b VALUES (20, 1, 1, 5), (21, 2, 2, 6)");
            execute("CREATE TABLE c (id INT, y INT)");
            execute("INSERT INTO c VALUES (30, 5), (31, 6)");
            assertQuery("""
                    SELECT o.id, t.aid, t.bid, t.cid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, s.id bid, c.id cid FROM a
                        JOIN (SELECT id, k, y FROM b WHERE x != o.x) s ON s.k = a.x
                        LEFT JOIN c ON c.y = s.y
                    ) t ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid\tcid
                            1\t11\t21\t31
                            2\t10\t20\t30
                            """);
            assertQuery("""
                    SELECT o.id, t.aid, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, s.id bid FROM a
                        JOIN (SELECT id, k, x FROM b WHERE k = o.x OR k = o.id) s ON s.x = a.x
                        WHERE s.k = o.x
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, t.cid, t.sid FROM o CROSS JOIN LATERAL (
                        SELECT c.id cid, s.id sid FROM c
                        LEFT JOIN (
                            SELECT n1.id, n1.y FROM b n1 JOIN (SELECT k FROM b WHERE k != o.x) n2 ON n2.k = n1.k
                        ) s ON s.y = c.y
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcid\tsid
                            1\t30\tnull
                            1\t31\t21
                            2\t30\t20
                            2\t31\tnull
                            """);
            assertQuery("""
                    SELECT o.id, t.cid, t.sid FROM o CROSS JOIN LATERAL (
                        SELECT c.id cid, s.id sid FROM c
                        JOIN (
                            SELECT n1.id, n1.y FROM b n1 JOIN (SELECT k FROM b WHERE k != o.x) n2 ON n2.k = n1.k
                        ) s ON s.y = c.y
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcid\tsid
                            1\t31\t21
                            2\t30\t20
                            """);
        });
    }

    @Test
    public void testSharedDomainSourceFreedAfterExplainAndFailedCreate() throws Exception {
        // #7803 section 3
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, 2)");
            createA("(10, 1), (11, 2)");
            execute("CREATE TABLE b (id INT, k INT, x INT, y INT)");
            execute("INSERT INTO b VALUES (20, 1, 1, 5), (21, 2, 2, 6)");
            execute("CREATE TABLE c (id INT, y INT)");
            execute("INSERT INTO c VALUES (30, 5), (31, 6)");
            printSql("""
                    EXPLAIN SELECT o.id, t.aid, t.bid, t.cid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, s.id bid, c.id cid FROM a
                        JOIN (SELECT id, k, y FROM b WHERE x != o.x) s ON s.k = a.x
                        LEFT JOIN c ON c.y = s.y
                    ) t
                    """);
            TestUtils.assertContains(sink, "Hash Left Outer Join");
            final String sql = """
                    CREATE TABLE ct AS (
                        SELECT o.id, t.aid, t.bid, t.cid FROM o CROSS JOIN LATERAL (
                            SELECT a.id aid, s.id bid, c.id cid FROM a
                            JOIN (SELECT id, k, y FROM b WHERE x != o.x) s ON s.k = a.x
                            LEFT JOIN c ON c.y = s.y
                        ) t
                    ), CAST(no_such_col AS LONG)
                    """;
            assertExceptionNoLeakCheck(sql, sql.indexOf("no_such_col"), "CAST column doesn't exist [column=no_such_col]");
        });
    }

    @Test
    public void testSharedSymbolDomainReadByMoreThanOneConsumer() throws Exception {
        // #7803 section 3
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x SYMBOL)");
            execute("INSERT INTO o VALUES (1, '1'), (2, '2')");
            execute("CREATE TABLE a (id INT, x SYMBOL)");
            execute("INSERT INTO a VALUES (10, '1'), (11, '2')");
            execute("CREATE TABLE b (id INT, k SYMBOL, x SYMBOL, y INT)");
            execute("INSERT INTO b VALUES (20, '1', '1', 5), (21, '2', '2', 6)");
            execute("CREATE TABLE c (id INT, y INT)");
            execute("INSERT INTO c VALUES (30, 5), (31, 6)");
            assertQuery("""
                    SELECT o.id, t.aid, t.bid, t.cid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, s.id bid, c.id cid FROM a
                        JOIN (SELECT id, k, y FROM b WHERE x != o.x) s ON s.k = a.x
                        LEFT JOIN c ON c.y = s.y
                    ) t ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid\tcid
                            1\t11\t21\t31
                            2\t10\t20\t30
                            """);
        });
    }

    @Test
    public void testSortAboveOrBelowCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            final String expected = """
                    id\tx
                    1\t1
                    1\t2
                    1\t7
                    2\t3
                    2\t4
                    2\t8
                    3\t5
                    3\t6
                    """;
            assertQuery("SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM t WHERE k = o.k ORDER BY x DESC) d ORDER BY o.id, d.x")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("SELECT o.id, d.x FROM o JOIN LATERAL (SELECT x FROM (SELECT x, k FROM t ORDER BY x DESC) d WHERE d.k = o.k) d ORDER BY o.id, d.x")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testStandaloneUnnestBeforeCorrelatedJoin() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            final String expected = """
                    id\tn
                    1\t14
                    2\t12
                    3\t10
                    4\t0
                    """;
            assertQuery("""
                    SELECT o.id, d.n FROM o JOIN LATERAL (
                        SELECT count() n FROM UNNEST(ARRAY[1.0, 2.0]) u(v) CROSS JOIN t WHERE t.x > o.k
                    ) d ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("""
                    SELECT o.id, d.n FROM o JOIN LATERAL (
                        SELECT count() n FROM long_sequence(1) CROSS JOIN UNNEST(ARRAY[1.0, 2.0]) u(v) CROSS JOIN t WHERE t.x > o.k
                    ) d ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testStandaloneUnnestOfOuterColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE arrs (id INT, arr DOUBLE[], w DOUBLE[])");
            execute("""
                    INSERT INTO arrs VALUES
                    (1, ARRAY[1.0, 2.0], ARRAY[10.0]),
                    (2, ARRAY[3.0], ARRAY[30.0, 31.0]),
                    (3, ARRAY[]::DOUBLE[], NULL),
                    (4, NULL, ARRAY[40.0])
                    """);
            final String standalone = "SELECT o.id, d.v FROM arrs o JOIN LATERAL (SELECT u.v FROM UNNEST(o.arr) u(v)) d";
            final String crossJoined = "SELECT o.id, d.v FROM arrs o JOIN LATERAL (SELECT u.v FROM long_sequence(1) CROSS JOIN UNNEST(o.arr) u(v)) d";
            final String values = """
                    id\tv
                    1\t1.0
                    1\t2.0
                    2\t3.0
                    """;
            assertQuery(standalone + " ORDER BY o.id, d.v").noLeakCheck().expectSize().returns(values);
            assertQuery(crossJoined + " ORDER BY o.id, d.v").noLeakCheck().expectSize().returns(values);
            final String plan = """
                    SelectedRecord
                        Hash Join
                          condition: d.__qdb_outer_ref__0_arr=o.arr
                            PageFrame
                                Row forward scan
                                Frame forward scan on: arrs
                            Hash
                                SelectedRecord
                                    Unnest
                                      columns: [v]
                                        Cross Join
                                            long_sequence count: 1
                                            Async Group By workers: 1
                                              keys: [__qdb_outer_ref__0_arr]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: arrs
                    """;
            assertQuery(standalone).noLeakCheck().assertsPlan(plan);
            assertQuery(crossJoined).noLeakCheck().assertsPlan(plan);
            assertQuery("SELECT * FROM arrs o JOIN LATERAL (SELECT * FROM UNNEST(o.arr) u(v)) d ORDER BY o.id, d.v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tarr\tw\tv
                            1\t[1.0,2.0]\t[10.0]\t1.0
                            1\t[1.0,2.0]\t[10.0]\t2.0
                            2\t[3.0]\t[30.0,31.0]\t3.0
                            """);
            assertQuery("SELECT o.id, d.* FROM arrs o JOIN LATERAL (SELECT * FROM UNNEST(o.arr, o.w) WITH ORDINALITY u(v, w, n)) d ORDER BY o.id, d.n")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tv\tw\tn
                            1\t1.0\t10.0\t1
                            1\t2.0\tnull\t2
                            2\t3.0\t30.0\t1
                            2\tnull\t31.0\t2
                            4\tnull\t40.0\t1
                            """);
            assertQuery("SELECT o.id, d.v FROM arrs o LEFT JOIN LATERAL (SELECT u.v FROM UNNEST(o.arr) u(v)) d ON true ORDER BY o.id, d.v")
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t1.0
                            1\t2.0
                            2\t3.0
                            3\tnull
                            4\tnull
                            """);
            assertQuery("SELECT o.id, d.n, d.s FROM arrs o JOIN LATERAL (SELECT count() n, sum(u.v) s FROM UNNEST(o.arr) u(v)) d ORDER BY o.id")
                    .noLeakCheck()
                    .returns("""
                            id\tn\ts
                            1\t2\t3.0
                            2\t1\t3.0
                            3\t0\tnull
                            4\t0\tnull
                            """);
        });
    }

    @Test
    public void testTwoInnerJoinedCorrelatedSubQueries() throws Exception {
        // #7803 section 4
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, 2)");
            createAKeyOnly("(10, 1)");
            createB("(20, 1, 1), (21, 1, 2)");
            execute("CREATE TABLE c (id INT, k INT, x INT)");
            execute("INSERT INTO c VALUES (30, 1, 1), (31, 1, 2)");
            assertQuery("""
                    SELECT o.id, t.aid, t.bid, t.cid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, q.id bid, r.id cid FROM a
                        JOIN (SELECT id, k FROM b WHERE x = o.x) q ON q.k = a.k
                        JOIN (SELECT id, k FROM c WHERE x = o.x) r ON r.k = q.k
                    ) t ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid\tcid
                            1\t10\t20\t30
                            2\t10\t21\t31
                            """);
        });
    }

    @Test
    public void testUnnestReadingOuterColumnBeforeEquatedInput() throws Exception {
        assertMemoryLeak(() -> {
            createOuterArrays();
            final String expected = """
                    id\tv
                    1\t1.0
                    1\t2.0
                    2\t3.0
                    5\t9.0
                    """;
            assertQuery("SELECT o.id, d.v FROM arrs o JOIN LATERAL (SELECT u.v FROM UNNEST(o.arr) u(v) CROSS JOIN arrs t WHERE t.arr = o.arr) d ORDER BY 1, 2")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("""
                    SELECT o.id, d.v FROM arrs o JOIN LATERAL (
                        SELECT u.v FROM long_sequence(1) CROSS JOIN UNNEST(o.arr) u(v) CROSS JOIN arrs t WHERE t.arr = o.arr
                    ) d ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testUnnestReadingOuterColumnBeforeRightOrFullJoin() throws Exception {
        assertMemoryLeak(() -> {
            createOuterArrays();
            execute("CREATE TABLE t (x DOUBLE)");
            execute("INSERT INTO t VALUES (1.0), (2.0), (3.0), (5.0)");
            final String rightRows = """
                    id\tv\tx
                    1\t1.0\t1.0
                    1\t2.0\t2.0
                    1\tnull\t3.0
                    1\tnull\t5.0
                    2\tnull\t1.0
                    2\tnull\t2.0
                    2\t3.0\t3.0
                    2\tnull\t5.0
                    3\tnull\t1.0
                    3\tnull\t2.0
                    3\tnull\t3.0
                    3\tnull\t5.0
                    4\tnull\t1.0
                    4\tnull\t2.0
                    4\tnull\t3.0
                    4\tnull\t5.0
                    5\tnull\t1.0
                    5\tnull\t2.0
                    5\tnull\t3.0
                    5\tnull\t5.0
                    """;
            assertOverOuterArrays("SELECT u.v, t.x FROM UNNEST(%s) u(v) RIGHT JOIN t ON t.x = u.v", rightRows);
            assertOverOuterArrays("SELECT u.v, t.x FROM long_sequence(1) CROSS JOIN UNNEST(%s) u(v) RIGHT JOIN t ON t.x = u.v", rightRows);
            final String fullRows = rightRows + "5\t9.0\tnull\n";
            assertOverOuterArrays("SELECT u.v, t.x FROM UNNEST(%s) u(v) FULL JOIN t ON t.x = u.v", fullRows);
            assertOverOuterArrays("SELECT u.v, t.x FROM long_sequence(1) CROSS JOIN UNNEST(%s) u(v) FULL JOIN t ON t.x = u.v", fullRows);
        });
    }

    @Test
    public void testWhereEqualityOnCorrelatedFullOrRightJoinSlave() throws Exception {
        // #7725, #7726
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o2 (id INT, k INT, x INT)");
            execute("CREATE TABLE a2 (id INT, k INT, x INT)");
            execute("CREATE TABLE b2 (id INT, k INT, x INT)");
            execute("INSERT INTO o2 VALUES (1, 5, 1)");
            execute("INSERT INTO a2 VALUES (10, 9, 9), (11, 8, 8)");
            execute("INSERT INTO b2 VALUES (20, 2, 1)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o2 o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a2 a FULL JOIN (SELECT id, k, x FROM b2 WHERE x = o.x) s ON a.k = s.x
                        WHERE s.k < o.k
                    ) l
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\tnull\t20
                            """);
            createOuter("(1, 1), (2, NULL)");
            createA("(10, 1)");
            createB("(20, 5, 1)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a FULL JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k
                        WHERE a.x = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\tnull
                            """);
            execute("INSERT INTO b VALUES (21, 1, 1), (22, 7, NULL)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a FULL JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k
                        WHERE a.x = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t21
                            2\tnull\t22
                            """);
        });
    }

    @Test
    public void testWhereEqualityOnFirstTableWithNonEqualityJoinedSubQuery() throws Exception {
        // #7803 section 13
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, 2)");
            createAKeyed("(10, 1, 1), (11, 2, 2)");
            createB("(20, 1, 2), (21, 2, 1)");
            execute("CREATE TABLE d (k INT)");
            execute("INSERT INTO d VALUES (1), (1), (2)");
            assertQuery("""
                    SELECT o.id, t.aid, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id bid FROM a
                        LEFT JOIN (SELECT id, k FROM b WHERE x != o.x) r ON r.k = a.k
                        WHERE a.x = o.x
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, t.aid, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id bid FROM a
                        JOIN (SELECT id, k FROM b WHERE x != o.x) r ON r.k = a.k
                        WHERE a.x = o.x
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, t.ax, t.c FROM o CROSS JOIN LATERAL (
                        SELECT a.id + o.x ax, w.c FROM a
                        CROSS JOIN LATERAL (SELECT count(*) c FROM d WHERE d.k = a.k) w
                        WHERE a.x = o.x
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tax\tc
                            1\t11\t2
                            2\t13\t1
                            """);
        });
    }

    @Test
    public void testWhereEqualityOnOuterDependentLeftJoinSlave() throws Exception {
        // #7732
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, NULL)");
            createA("(10, 1), (11, 5)");
            createB("(20, 1, 1)");
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a LEFT JOIN b ON a.x = b.k AND b.k = o.x WHERE b.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t10\tnull
                            2\t11\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a LEFT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE s.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            2\t10\tnull
                            2\t11\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o LEFT JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a LEFT JOIN b ON a.x = b.k AND b.k = o.x WHERE b.k = o.x
                    ) l ON true ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t10\tnull
                            2\t11\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o LEFT JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a LEFT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE s.k = o.x
                    ) l ON true ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            2\t10\tnull
                            2\t11\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a LEFT JOIN b ON a.x = b.k WHERE b.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t11\tnull
                            """);
        });
    }

    @Test
    public void testWhereEqualityOnTableJoinedAfterCorrelatedSubQuery() throws Exception {
        // #7803 section 14
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, 2)");
            createAKeyOnly("(10, 1), (11, 2)");
            createB("(20, 1, 1), (21, 2, 2)");
            execute("CREATE TABLE d (id INT, k INT, x INT)");
            execute("INSERT INTO d VALUES (40, 1, 1), (41, 2, 1), (42, 2, 2)");
            assertQuery("""
                    SELECT o.id, t.aid, t.rid, t.sid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id rid, s.id sid FROM a
                        LEFT JOIN (SELECT id, k FROM b WHERE x = o.x) r ON r.k = a.k
                        LEFT JOIN d s ON s.k = a.k
                        WHERE s.x = o.x
                    ) t ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\trid\tsid
                            1\t10\t20\t40
                            1\t11\tnull\t41
                            2\t11\t21\t42
                            """);
            assertQuery("""
                    SELECT o.id, t.aid, t.sid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a
                        LEFT JOIN d s ON s.k = a.k
                        WHERE s.x = o.x
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t40
                            1\t11\t41
                            2\t11\t42
                            """);
            assertQuery("""
                    SELECT o.id, t.aid, t.rid, t.sid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id rid, s.id sid FROM a
                        JOIN (SELECT id, k FROM b WHERE x = o.x) r ON r.k = a.k
                        LEFT JOIN d s ON s.k = a.k
                        WHERE s.x = o.x
                    ) t ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\trid\tsid
                            1\t10\t20\t40
                            2\t11\t21\t42
                            """);
        });
    }

    @Test
    public void testWildcardOverFullJoinHidesCarriers() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT * FROM orders o JOIN LATERAL (
                        SELECT * FROM trades t FULL JOIN refunds r ON t.x = r.k AND r.k = o.k
                    ) l ORDER BY 1, 3, 5
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tk\tid1\tx\tid11\tk1
                            1\t1\tnull\tnull\t101\t2
                            1\t1\t10\t1\t100\t1
                            2\t2\tnull\tnull\t100\t1
                            2\t2\tnull\tnull\t101\t2
                            2\t2\t10\t1\tnull\tnull
                            """);
        });
    }

    @Test
    public void testWindowAboveOrBelowCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            createCorrelationPointTables();
            assertQuery("SELECT o.id, d.x, d.rn FROM o JOIN LATERAL (SELECT x, row_number() OVER (ORDER BY x) rn FROM t WHERE k = o.k) d ORDER BY o.id, d.x")
                    .noLeakCheck()
                    .returns("""
                            id\tx\trn
                            1\t1\t1
                            1\t2\t2
                            1\t7\t3
                            2\t3\t1
                            2\t4\t2
                            2\t8\t3
                            3\t5\t1
                            3\t6\t2
                            """);
            assertQuery("""
                    SELECT o.id, d.x, d.rn FROM o JOIN LATERAL (
                        SELECT x, rn FROM (SELECT x, k, row_number() OVER (ORDER BY x) rn FROM t) d WHERE d.k = o.k
                    ) d ORDER BY o.id, d.x
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tx\trn
                            1\t1\t1
                            1\t2\t2
                            1\t7\t7
                            2\t3\t3
                            2\t4\t4
                            2\t8\t8
                            3\t5\t5
                            3\t6\t6
                            """);
        });
    }

    @Test
    public void testWindowAndLatestOnAboveJoinedCorrelatedSubQuery() throws Exception {
        // #7803 section 7
        assertMemoryLeak(() -> {
            createOuter("(1, 1), (2, 2)");
            execute("CREATE TABLE a (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO a VALUES (10, 1, '2024-01-01T00:00:00.000000Z'), (11, 2, '2024-01-01T00:00:01.000000Z')");
            createB("(20, 1, 1), (21, 2, 1), (22, 1, 2), (23, 2, 2)");
            assertQuery("""
                    SELECT o.id, t.bid, t.rn FROM o CROSS JOIN LATERAL (
                        SELECT bid, row_number() OVER (ORDER BY bid) rn FROM (
                            SELECT q.id bid FROM a JOIN (SELECT id, k FROM b WHERE x = o.x) q ON q.k = a.k
                        )
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\trn
                            1\t20\t1
                            1\t21\t2
                            2\t22\t1
                            2\t23\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.c FROM o CROSS JOIN LATERAL (
                        SELECT bid, count(*) OVER () c FROM (
                            SELECT q.id bid FROM a JOIN (SELECT id, k FROM b WHERE x = o.x) q ON q.k = a.k
                        )
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\tc
                            1\t20\t2
                            1\t21\t2
                            2\t22\t2
                            2\t23\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.aid, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT aid, bid FROM (
                            (SELECT a.id aid, a.k ak, q.id bid, a.ts FROM a JOIN (SELECT id, k FROM b WHERE x = o.x) q ON q.k = a.k) TIMESTAMP(ts)
                        ) LATEST ON ts PARTITION BY ak
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            1\t11\t21
                            2\t10\t22
                            2\t11\t23
                            """);
        });
    }

    @Test
    public void testWindowJoinOverCorrelatedSlave() throws Exception {
        assertMemoryLeak(() -> {
            createTemporalJoinTables();
            final String prevailingSums = """
                    k\tid\tx
                    null\t1\tnull
                    null\t2\t30
                    null\t3\t61
                    null\t4\t61
                    1\t1\t10
                    1\t2\t10
                    1\t3\t33
                    1\t4\t33
                    2\t1\tnull
                    2\t2\t20
                    2\t3\t41
                    2\t4\t41
                    3\t1\tnull
                    3\t2\tnull
                    3\t3\tnull
                    3\t4\tnull
                    """;
            assertOverCorrelatedSlave(
                    "SELECT b.id, sum(c.x) x FROM b WINDOW JOIN (SELECT * FROM c WHERE k = %s) c RANGE BETWEEN 20 SECONDS PRECEDING AND CURRENT ROW",
                    prevailingSums
            );
            assertQuery("""
                    SELECT a.k, l.id, l.x FROM a JOIN LATERAL (
                        SELECT b.id, sum(c.x) x FROM b WINDOW JOIN c ON c.k = a.k RANGE BETWEEN 20 SECONDS PRECEDING AND CURRENT ROW
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(prevailingSums);
            assertOverCorrelatedSlave(
                    "SELECT b.id, sum(c.x) x FROM b WINDOW JOIN (SELECT * FROM c WHERE k = %s) c ON (k) RANGE BETWEEN 20 SECONDS PRECEDING AND CURRENT ROW",
                    """
                            k\tid\tx
                            null\t1\tnull
                            null\t2\tnull
                            null\t3\tnull
                            null\t4\t61
                            1\t1\t10
                            1\t2\tnull
                            1\t3\t33
                            1\t4\tnull
                            2\t1\tnull
                            2\t2\t20
                            2\t3\tnull
                            2\t4\tnull
                            3\t1\tnull
                            3\t2\tnull
                            3\t3\tnull
                            3\t4\tnull
                            """
            );
            assertOverCorrelatedSlave(
                    "SELECT b.id, sum(c.x) x FROM b WINDOW JOIN (SELECT * FROM c WHERE k = %s) c RANGE BETWEEN 20 SECONDS PRECEDING AND CURRENT ROW EXCLUDE PREVAILING",
                    """
                            k\tid\tx
                            null\t1\tnull
                            null\t2\t30
                            null\t3\t31
                            null\t4\t31
                            1\t1\t10
                            1\t2\tnull
                            1\t3\t23
                            1\t4\t23
                            2\t1\tnull
                            2\t2\t20
                            2\t3\t21
                            2\t4\t21
                            3\t1\tnull
                            3\t2\tnull
                            3\t3\tnull
                            3\t4\tnull
                            """
            );
        });
    }

    private void assertFullJoinCarrier(String type, String value1, String value2) throws Exception {
        execute("CREATE TABLE o (id INT, v " + type + ")");
        execute("CREATE TABLE a (id INT, v " + type + ")");
        execute("CREATE TABLE b (id INT, v " + type + ")");
        execute("INSERT INTO o VALUES (1, " + value1 + "), (2, " + value2 + ")");
        execute("INSERT INTO a VALUES (10, " + value1 + ")");
        execute("INSERT INTO b VALUES (20, " + value1 + "), (21, " + value2 + ")");
        assertQuery("""
                SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                    SELECT a.id aid, b.id bid FROM a FULL JOIN b ON a.v = b.v AND b.v = o.v
                ) l ORDER BY 1, 2, 3
                """)
                .noLeakCheck()
                .expectSize()
                .returns(FULL_JOIN_CARRIER_ROWS);
        execute("DROP TABLE o");
        execute("DROP TABLE a");
        execute("DROP TABLE b");
    }

    private void assertLimitBelowCorrelation(String sql, String expected) throws Exception {
        assertQuery(sql)
                .noLeakCheck()
                .returns(expected);
    }

    private void assertNullOuterValueFindsDomainRow(String type, String value1, String value2) throws Exception {
        execute("CREATE TABLE o (id INT, x INT)");
        execute("INSERT INTO o VALUES (1, 1), (2, 2)");
        execute("CREATE TABLE c (v " + type + ", x INT)");
        execute("INSERT INTO c VALUES (" + value1 + ", 1)");
        execute("CREATE TABLE d (id INT, v " + type + ")");
        execute("INSERT INTO d VALUES (1, " + value1 + "), (2, " + value2 + ")");
        assertQuery("""
                SELECT o.id, t.did FROM o LEFT JOIN c ON c.x = o.x CROSS JOIN LATERAL (
                    SELECT d.id did FROM d WHERE d.v != c.v
                ) t ORDER BY 1, 2
                """)
                .noLeakCheck()
                .expectSize()
                .returns("""
                        id\tdid
                        1\t2
                        2\t1
                        2\t2
                        """);
        execute("DROP TABLE o");
        execute("DROP TABLE c");
        execute("DROP TABLE d");
    }

    private void assertOverCorrelatedHorizonSlave(String body, String expected) throws Exception {
        assertQuery("SELECT o.id, d.* FROM o JOIN LATERAL (" + body.formatted("o.k") + ") d ORDER BY 1, 2")
                .noLeakCheck()
                .returns(expected);
        assertQuery("""
                SELECT * FROM (
                    SELECT 1 id, * FROM (%s)
                    UNION ALL SELECT 2, * FROM (%s)
                    UNION ALL SELECT 3, * FROM (%s)
                    UNION ALL SELECT 4, * FROM (%s)
                    UNION ALL SELECT 5, * FROM (%s)
                ) ORDER BY 1, 2
                """.formatted(body.formatted("1"), body.formatted("2"), body.formatted("3"), body.formatted("10"), body.formatted("NULL")))
                .noLeakCheck()
                .expectSize()
                .returns(expected);
    }

    private void assertOverCorrelatedSlave(String body, String expected) throws Exception {
        assertQuery("SELECT a.k, l.id, l.x FROM a JOIN LATERAL (" + body.formatted("a.k") + ") l ORDER BY 1, 2")
                .noLeakCheck()
                .expectSize()
                .returns(expected);
        assertQuery("SELECT a.k, l.id, l.x FROM a LEFT JOIN LATERAL (" + body.formatted("a.k") + ") l ON true ORDER BY 1, 2")
                .noLeakCheck()
                .returns(expected);
    }

    private void assertOverCorrelatedSlaveAndPerKeyUnion(String body, String expected) throws Exception {
        assertOverCorrelatedSlave(body, expected);
        assertQuery("""
                SELECT * FROM (
                    SELECT 1 k, id, x FROM (%s)
                    UNION ALL SELECT 2, id, x FROM (%s)
                    UNION ALL SELECT 3, id, x FROM (%s)
                    UNION ALL SELECT NULL::INT, id, x FROM (%s)
                ) ORDER BY 1, 2
                """.formatted(body.formatted("1"), body.formatted("2"), body.formatted("3"), body.formatted("NULL")))
                .noLeakCheck()
                .expectSize()
                .returns(expected);
    }

    private void assertOverOuterArrays(String body, String expected) throws Exception {
        assertQuery("SELECT o.id, d.v, d.x FROM arrs o JOIN LATERAL (" + body.formatted("o.arr") + ") d ORDER BY 1, 3, 2")
                .noLeakCheck()
                .expectSize()
                .returns(expected);
        assertQuery("""
                SELECT * FROM (
                    SELECT 1 id, v, x FROM (%s)
                    UNION ALL SELECT 2, v, x FROM (%s)
                    UNION ALL SELECT 3, v, x FROM (%s)
                    UNION ALL SELECT 4, v, x FROM (%s)
                    UNION ALL SELECT 5, v, x FROM (%s)
                ) ORDER BY 1, 3, 2
                """.formatted(body.formatted("ARRAY[1.0, 2.0]"), body.formatted("ARRAY[3.0]"), body.formatted("ARRAY[]::DOUBLE[]"),
                body.formatted("NULL::DOUBLE[]"), body.formatted("ARRAY[9.0]")))
                .noLeakCheck()
                .returns(expected);
    }

    private void assertQueryReturns(SqlCompiler compiler, String sql, String expected) throws Exception {
        assertQueryReturns(compiler, sql, expected, false, true);
    }

    private void assertQueryReturns(SqlCompiler compiler, String sql, String expected, boolean isSizeKnown, boolean supportsRandomAccess) throws Exception {
        assertQuery(sql)
                .withCompiler(compiler)
                .withContext(sqlExecutionContext)
                .noLeakCheck()
                .expectSize(isSizeKnown)
                .supportsRandomAccess(supportsRandomAccess)
                .returns(expected);
    }

    private void assertSpliceCorrelationRejected(String sql) throws Exception {
        assertQuery(sql).noLeakCheck().fails(sql.indexOf("SPLICE"), "outer column reference at or before a SPLICE join is not supported in a LATERAL sub-query");
    }

    private void createA(String rows) throws Exception {
        execute("CREATE TABLE a (id INT, x INT)");
        execute("INSERT INTO a VALUES " + rows);
    }

    private void createAKeyOnly(String rows) throws Exception {
        execute("CREATE TABLE a (id INT, k INT)");
        execute("INSERT INTO a VALUES " + rows);
    }

    private void createAKeyed(String rows) throws Exception {
        execute("CREATE TABLE a (id INT, k INT, x INT)");
        execute("INSERT INTO a VALUES " + rows);
    }

    private void createB(String rows) throws Exception {
        execute("CREATE TABLE b (id INT, k INT, x INT)");
        execute("INSERT INTO b VALUES " + rows);
    }

    private void createCommaGroupTables() throws Exception {
        execute("CREATE TABLE a (k INT)");
        execute("CREATE TABLE b (k INT)");
        execute("CREATE TABLE c (k INT)");
        execute("INSERT INTO a VALUES (7)");
        execute("INSERT INTO b VALUES (1)");
        execute("INSERT INTO c VALUES (1), (2)");
    }

    private void createCorrelationPointTables() throws Exception {
        execute("CREATE TABLE t (x INT, k INT)");
        execute("INSERT INTO t VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 3), (6, 3), (7, 1), (8, 2)");
        execute("CREATE TABLE o (id INT, k INT)");
        execute("INSERT INTO o VALUES (1, 1), (2, 2), (3, 3), (4, 10)");
        execute("CREATE TABLE e (x INT, k INT, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO e VALUES
                (1, 1, 'a', '2024-01-01T00:00:00.000000Z'),
                (2, 1, 'b', '2024-01-01T01:00:00.000000Z'),
                (3, 2, 'a', '2024-01-01T02:00:00.000000Z'),
                (4, 2, 'b', '2024-01-01T03:00:00.000000Z'),
                (5, 3, 'a', '2024-01-01T04:00:00.000000Z'),
                (6, 3, 'b', '2024-01-01T05:00:00.000000Z'),
                (7, 1, 'a', '2024-01-01T06:00:00.000000Z'),
                (8, 2, 'b', '2024-01-01T07:00:00.000000Z')
                """);
        execute("CREATE TABLE q (s SYMBOL, k INT, bid DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO q VALUES
                ('a', 1, 10.0, '2024-01-01T00:30:00.000000Z'),
                ('b', 2, 20.0, '2024-01-01T01:30:00.000000Z'),
                ('a', 3, 30.0, '2024-01-01T03:30:00.000000Z'),
                ('b', 1, 40.0, '2024-01-01T05:30:00.000000Z')
                """);
    }

    private void createOrdersTradesRefunds() throws Exception {
        execute("CREATE TABLE orders (id INT, k INT)");
        execute("CREATE TABLE trades (id INT, x INT)");
        execute("CREATE TABLE refunds (id INT, k INT)");
        execute("CREATE TABLE xs (k INT, v INT)");
        execute("INSERT INTO orders VALUES (1, 1), (2, 2)");
        execute("INSERT INTO trades VALUES (10, 1)");
        execute("INSERT INTO refunds VALUES (100, 1), (101, 2)");
        execute("INSERT INTO xs VALUES (1, 100)");
    }

    private void createOuter(String rows) throws Exception {
        execute("CREATE TABLE o (id INT, x INT)");
        execute("INSERT INTO o VALUES " + rows);
    }

    private void createOuterArrays() throws Exception {
        execute("CREATE TABLE arrs (id INT, arr DOUBLE[])");
        execute("""
                INSERT INTO arrs VALUES
                (1, ARRAY[1.0, 2.0]),
                (2, ARRAY[3.0]),
                (3, ARRAY[]::DOUBLE[]),
                (4, NULL),
                (5, ARRAY[9.0])
                """);
    }

    private void createOuterKeyed(String rows) throws Exception {
        execute("CREATE TABLE o (id INT, k INT, x INT)");
        execute("INSERT INTO o VALUES " + rows);
    }

    private void createTemporalJoinTables() throws Exception {
        execute("CREATE TABLE a (k INT)");
        execute("INSERT INTO a VALUES (1), (2), (3), (null)");
        execute("CREATE TABLE b (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("""
                INSERT INTO b VALUES
                (1, 1, '2024-01-01T00:00:10.000000Z'),
                (2, 2, '2024-01-01T00:00:30.000000Z'),
                (3, 1, '2024-01-01T00:00:50.000000Z'),
                (4, null, '2024-01-01T00:00:50.000000Z')
                """);
        execute("CREATE TABLE c (k INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("""
                INSERT INTO c VALUES
                (1, 10, '2024-01-01T00:00:00.000000Z'),
                (2, 20, '2024-01-01T00:00:20.000000Z'),
                (null, 30, '2024-01-01T00:00:25.000000Z'),
                (1, 11, '2024-01-01T00:00:40.000000Z'),
                (2, 21, '2024-01-01T00:00:45.000000Z'),
                (1, 12, '2024-01-01T00:00:50.000000Z'),
                (null, 31, '2024-01-01T00:00:50.000000Z')
                """);
    }
}
