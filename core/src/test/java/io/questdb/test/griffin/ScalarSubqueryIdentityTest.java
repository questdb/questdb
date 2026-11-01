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
 * Expressions that differ only in their scalar sub-queries are different expressions wherever the
 * engine matches or deduplicates expressions: grouping keys, DISTINCT, aggregates and ORDER BY.
 * The same sub-query, written twice with the same tokens or read twice from one declaration, stays
 * one expression, whatever the case of its keywords and identifiers and the spacing between them.
 */
public class ScalarSubqueryIdentityTest extends AbstractCairoTest {

    @Test
    public void testAggregatesKeepDistinctSubqueries() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT sum(x * (x = (SELECT 1))::INT) a, sum(x * (x = (SELECT 2))::INT) b FROM long_sequence(3)")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            a\tb
                            1\t2
                            """);
            assertQuery("""
                    SELECT x = (SELECT 1) p, sum(x * (x = (SELECT 2))::INT) a, sum(x * (x = (SELECT 3))::INT) b
                    FROM long_sequence(3)
                    ORDER BY p
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            p\ta\tb
                            false\t2\t3
                            true\t0\t0
                            """);
        });
    }

    @Test
    public void testDeclaredSubqueryDiffersFromEqualTextWrittenInPlace() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT 1 a)");
            // The declaration reads table t, while the same text written in place reads the CTE t.
            assertQuery("""
                    DECLARE @q := (SELECT max(a) FROM t)
                    WITH t AS (SELECT 5 a)
                    SELECT x = @q p, x = (SELECT max(a) FROM t) q, count() n
                    FROM long_sequence(5)
                    ORDER BY p, q
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            p\tq\tn
                            false\tfalse\t3
                            false\ttrue\t1
                            true\tfalse\t1
                            """);
        });
    }

    @Test
    public void testDistinctKeepsDistinctSubqueries() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT DISTINCT x = (SELECT 1) p, x = (SELECT 2) q FROM long_sequence(3) ORDER BY p, q")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        p\tq
                        false\tfalse
                        false\ttrue
                        true\tfalse
                        """));
    }

    @Test
    public void testExplicitGroupingKeepsDistinctSubqueries() throws Exception {
        final String expected = """
                p\tq\tn
                false\tfalse\t1
                false\ttrue\t1
                true\tfalse\t1
                """;
        assertMemoryLeak(() -> {
            assertQuery("SELECT x = (SELECT 1) p, x = (SELECT 2) q, count() n FROM long_sequence(3) GROUP BY p, q ORDER BY p, q")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT x = (SELECT 1) p, x = (SELECT 2) q, count() n FROM long_sequence(3) GROUP BY 1, 2 ORDER BY 1, 2")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("""
                    SELECT x = (SELECT 1) p, x = (SELECT 2) q, count() n
                    FROM long_sequence(3)
                    GROUP BY x = (SELECT 1), x = (SELECT 2)
                    ORDER BY p, q
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("""
                    DECLARE @q := (SELECT 1), @r := (SELECT 2)
                    SELECT x = @q p, x = @r q, count() n
                    FROM long_sequence(3)
                    GROUP BY x = @q, x = @r
                    ORDER BY p, q
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testGroupingKeyOfAnotherSubqueryDoesNotCoverSelectedExpression() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT x = (SELECT 1) p, count() n FROM long_sequence(3) GROUP BY x = (SELECT 2)")
                .noLeakCheck()
                .fails(7, "column must appear in GROUP BY clause or aggregate function"));
    }

    @Test
    public void testImplicitGroupingKeepsDistinctSubqueries() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT x = (SELECT 1) p, x = (SELECT 2) q, count() n FROM long_sequence(3) ORDER BY p, q")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            p\tq\tn
                            false\tfalse\t1
                            false\ttrue\t1
                            true\tfalse\t1
                            """);
            assertQuery("SELECT x = (SELECT 1) p, x = (SELECT 2 FROM long_sequence(1) WHERE false) q, count() n FROM long_sequence(3) ORDER BY p, q")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            p\tq\tn
                            false\tfalse\t2
                            true\tfalse\t1
                            """);
            // the second read of w parses the CTE again
            assertQuery("""
                    WITH w AS (SELECT 1 a)
                    SELECT x = (SELECT max(w1.a) FROM w w1 CROSS JOIN w w2) p, x = (SELECT max(w1.a) + 1 FROM w w1 CROSS JOIN w w2) q, count() n
                    FROM long_sequence(3)
                    ORDER BY p, q
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            p\tq\tn
                            false\tfalse\t1
                            false\ttrue\t1
                            true\tfalse\t1
                            """);
        });
    }

    @Test
    public void testOrderByKeepsDistinctSubqueries() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT x, x = (SELECT 1) p FROM long_sequence(3) ORDER BY x = (SELECT 2), x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tp
                            1\ttrue
                            3\tfalse
                            2\tfalse
                            """);
            assertQuery("SELECT x, x = (SELECT 1) p, row_number() OVER () r FROM long_sequence(3) ORDER BY x = (SELECT 2), x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tp\tr
                            1\ttrue\t1
                            3\tfalse\t3
                            2\tfalse\t2
                            """);
            assertQuery("SELECT x = (SELECT 1) p, x = (SELECT 2) q, sum(x) s FROM long_sequence(3) ORDER BY 1, 2")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            p\tq\ts
                            false\tfalse\t3
                            false\ttrue\t2
                            true\tfalse\t1
                            """);
        });
    }

    @Test
    public void testSameSubqueryMatchesGroupingKey() throws Exception {
        final String expected = """
                p\tn
                false\t2
                true\t1
                """;
        assertMemoryLeak(() -> {
            assertQuery("SELECT x = (SELECT 1) p, count() n FROM long_sequence(3) GROUP BY x = (SELECT 1) ORDER BY p")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT x = (SELECT 1 ) p, count() n FROM long_sequence(3) GROUP BY x = (SELECT 1) ORDER BY x = (SELECT 1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE @q := (SELECT 1) SELECT x = @q p, count() n FROM long_sequence(3) GROUP BY x = @q ORDER BY x = @q")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE @e := x = (SELECT 1) SELECT @e p, count() n FROM long_sequence(3) GROUP BY @e ORDER BY p")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("""
                    WITH w AS (SELECT 1 a)
                    SELECT x = (SELECT max(w1.a) FROM w w1 CROSS JOIN w w2) p, count() n
                    FROM long_sequence(3)
                    GROUP BY x = (SELECT max(w1.a) FROM w w1 CROSS JOIN w w2)
                    ORDER BY p
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT x = (SELECT 1) p, x = (SELECT 1) q, count() n FROM long_sequence(3) ORDER BY p, q")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            p\tq\tn
                            false\tfalse\t2
                            true\ttrue\t1
                            """);
        });
    }

    @Test
    public void testSameSubqueryWithOtherCaseAndSpacingMatchesGroupingKey() throws Exception {
        final String expected = """
                p\tn
                false\t2
                true\t1
                """;
        assertMemoryLeak(() -> {
            assertQuery("SELECT x = (SELECT 1) p, count() n FROM long_sequence(3) GROUP BY x = (select 1) ORDER BY X = (Select 1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("""
                    SELECT x = (SELECT max(x) FROM long_sequence(1)) p, count() n
                    FROM long_sequence(3)
                    GROUP BY x = (select  MAX(X) /* comment */
                                  from LONG_SEQUENCE(1))
                    ORDER BY p
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("""
                    SELECT x = (SELECT max("X") FROM long_sequence(1)) p, count() n
                    FROM long_sequence(3)
                    GROUP BY x = (SELECT max("x") FROM long_sequence(1))
                    ORDER BY p
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE @v := 1 SELECT x = (SELECT @v) p, count() n FROM long_sequence(3) GROUP BY x = (SELECT @V) ORDER BY p")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testSubqueriesDifferingInCaseSensitiveTokensStayDistinct() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("""
                    SELECT x = (SELECT 1 FROM long_sequence(1) WHERE 'a' = 'a') p, x = (SELECT 1 FROM long_sequence(1) WHERE 'A' = 'a') q, count() n
                    FROM long_sequence(3)
                    ORDER BY p, q
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            p\tq\tn
                            false\tfalse\t2
                            true\tfalse\t1
                            """);
            assertQuery("SELECT x = (SELECT 1 FROM long_sequence(1) WHERE 'a' = 'a') p, count() n FROM long_sequence(3) GROUP BY x = (SELECT 1 FROM long_sequence(1) WHERE 'A' = 'a')")
                    .noLeakCheck()
                    .fails(7, "column must appear in GROUP BY clause or aggregate function");
            execute("CREATE TABLE t AS (SELECT timestamp_sequence('2024-01-01T00:00:00', 60_000_000) ts FROM long_sequence(2)) TIMESTAMP(ts)");
            // two one-minute buckets against one one-month bucket
            assertQuery("""
                    SELECT x = (SELECT count() FROM (SELECT count() FROM t SAMPLE BY 1m)) p, x = (SELECT count() FROM (SELECT count() FROM t SAMPLE BY 1M)) q, count() n
                    FROM long_sequence(3)
                    ORDER BY p, q
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            p\tq\tn
                            false\tfalse\t1
                            false\ttrue\t1
                            true\tfalse\t1
                            """);
        });
    }
}
