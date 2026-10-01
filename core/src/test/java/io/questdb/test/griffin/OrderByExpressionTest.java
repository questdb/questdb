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
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class OrderByExpressionTest extends AbstractCairoTest {

    @Test
    public void testOrderByBinaryFails() throws Exception {
        assertQuery("select b from (select rnd_bin(10, 20, 2) b from long_sequence(10)) order by b desc")
                .fails(76, "BINARY is not a supported type in ORDER BY clause");
    }

    @Test
    public void testOrderByColumnInJoinedSubquery() throws Exception {
        assertQuery("""
                select * from\s
                (
                  selecT x from long_sequence(10)\s
                )
                cross join\s
                (
                  select * from\s
                  (
                    selecT x*x as oth from long_sequence(10) order by x desc limit 5\s
                  )
                )
                order by x*2 asc
                limit 3""")
                .ddl(null)
                .returns("""
                        x\toth
                        1\t100
                        1\t81
                        1\t64
                        """);
    }

    // fails with duplicate column : column because alias created for 'x*x' clashes with one created for x+rnd_int(1,10,0)*0
    // TODO: test with order by x*2 in outer query
    @Test
    @Ignore
    public void testOrderByExpressionInJoinedSubquery() throws Exception {
        assertQuery("""
                select * from\s
                (
                  select x from long_sequence(10)\s
                )
                cross join\s
                (
                    select x*x,5*x from long_sequence(10) order by x+rnd_int(1,10,0)*0 desc limit 5\s
                )
                order by x*2  asc
                limit 3""")
                .ddl(null)
                .expectSize()
                .returns("""
                        x\tcolumn\tcolumn1
                        1\t100\t50
                        1\t81\t45
                        1\t64\t40
                        """);
    }

    @Test
    public void testOrderByExpressionInNestedQuery() throws Exception {
        assertQuery("""
                select * from\s
                (
                  select x from long_sequence(10) order by x/2 desc limit 5\s
                )
                order by x*2 asc
                limit 3""")
                .ddl(null)
                .expectSize()
                .returns("x\n6\n7\n8\n");
    }

    @Test
    public void testOrderByExpressionWhenColumnHasAliasInJoinedSubquery() throws Exception {
        assertQuery("""
                select * from\s
                (
                  select x from long_sequence(10)\s
                )
                cross join\s
                (
                    select x*x as ext from long_sequence(10) order by x+rnd_int(1,10,0)*0 desc limit 5\s
                )
                order by x*2 asc, ext desc
                limit 3""")
                .ddl(null)
                .expectSize()
                .returns("x\text\n1\t100\n1\t81\n1\t64\n");
    }

    @Test
    public void testOrderByExpressionWithDuplicatesMaintainsOriginalOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab as (select x x, x%2 y from long_sequence(10))");

            assertQuery("select * from tab order by x/x")
                    .expectSize()
                    .returns("""
                            x\ty
                            1\t1
                            2\t0
                            3\t1
                            4\t0
                            5\t1
                            6\t0
                            7\t1
                            8\t0
                            9\t1
                            10\t0
                            """);

            assertQuery("select * from tab order by y, x")
                    .expectSize()
                    .returns("""
                            x\ty
                            2\t0
                            4\t0
                            6\t0
                            8\t0
                            10\t0
                            1\t1
                            3\t1
                            5\t1
                            7\t1
                            9\t1
                            """);

            assertQuery("select * from (select t2.* from tab t1 cross join tab t2 limit 10) order by y")
                    .expectSize()
                    .returns("""
                            x\ty
                            2\t0
                            4\t0
                            6\t0
                            8\t0
                            10\t0
                            1\t1
                            3\t1
                            5\t1
                            7\t1
                            9\t1
                            """);

            assertQuery("select * from (select t2.* from tab t1 cross join tab t2 limit 10) order by x/x")
                    .expectSize()
                    .returns("""
                            x\ty
                            1\t1
                            2\t0
                            3\t1
                            4\t0
                            5\t1
                            6\t0
                            7\t1
                            8\t0
                            9\t1
                            10\t0
                            """);
        });
    }

    @Test
    public void testOrderByExpressionWithFunctionCallInNestedQuery() throws Exception {
        assertQuery("""
                select * from\s
                (
                    select x from long_sequence(10) order by x+rnd_int(1,10,0)*0 desc limit 5\s
                )
                order by x*2 asc
                limit 3""")
                .ddl(null)
                .expectSize()
                .returns("x\n6\n7\n8\n");
    }

    @Test
    public void testOrderByExpressionWithFunctionCallInWithClause() throws Exception {
        assertQuery("""
                with q as (select x from long_sequence(10) order by x+rnd_int(1,10,0)*0 desc limit 5 )\s
                select * from q
                order by x*2 asc
                limit 3""")
                .ddl(null)
                .expectSize()
                .returns("x\n6\n7\n8\n");
    }

    @Test
    public void testOrderByIntervalFails() throws Exception {
        assertQuery("select i from (" +
                "  (select interval(100000,200000) i) " +
                "  union all " +
                "  (select interval(100000,200000) i) " +
                "  union all " +
                "  (select null::interval i)" +
                ") " +
                "order by i desc")
                .fails(151, "INTERVAL is not a supported type in ORDER BY clause");
    }

    @Test
    public void testOrderByNumericColumnThatDoesExist() throws Exception {
        assertQuery("""
                SELECT * FROM (
                  SELECT 456 AS "5"
                  UNION ALL\s
                  SELECT 789 AS "5"
                  UNION ALL\s
                  SELECT 123 AS "5"
                )
                ORDER BY 5""")
                .expectSize()
                .returns("""
                        5
                        123
                        456
                        789
                        """);

        assertQuery("""
                SELECT * FROM (
                  SELECT 456 AS "5"
                  UNION ALL\s
                  SELECT 789 AS "5"
                  UNION ALL\s
                  SELECT 123 AS "5"
                )
                ORDER BY 1""")
                .expectSize()
                .returns("""
                        5
                        123
                        456
                        789
                        """);
    }

    @Test
    public void testOrderByNumericColumnThatDoesNotExist() throws Exception {
        assertQuery("""
                SELECT * FROM (
                  SELECT 456 AS "5"
                  UNION ALL\s
                  SELECT 789 AS "5"
                  UNION ALL\s
                  SELECT 123 AS "5"
                )
                ORDER BY 6""")
                .fails(113, "order column position is out of range [max=1]");
    }

    @Test
    public void testOrderByTwoColumnsInJoin() throws Exception {
        assertQuery("select * " +
                "from (" +
                "  select b.*" +
                "  from (select 42 id) a " +
                "  left join (x union all (select 0 id, 'foo0' s1, 'bar0')) b on a.id = b.id" +
                ")" +
                "order by s1, s2")
                .ddl("create table x as (select 42 id, rnd_str('foo1','foo2','foo3') s1, rnd_str('bar1','bar2','bar3') s2 from long_sequence(10))")
                .returns("""
                        id\ts1\ts2
                        42\tfoo1\tbar1
                        42\tfoo1\tbar2
                        42\tfoo1\tbar2
                        42\tfoo1\tbar2
                        42\tfoo2\tbar1
                        42\tfoo2\tbar2
                        42\tfoo2\tbar3
                        42\tfoo2\tbar3
                        42\tfoo3\tbar2
                        42\tfoo3\tbar3
                        """);
    }

    @Test
    public void testOrderByTwoExpressions() throws Exception {
        assertQuery("select x from long_sequence(10) order by x/100, x*x desc  limit 5")
                .ddl(null)
                .expectSize()
                .returns("x\n10\n9\n8\n7\n6\n");
    }

    @Test
    public void testOrderByTwoExpressionsInNestedQuery() throws Exception {
        assertQuery("""
                select * from\s
                (
                  select x from long_sequence(10) order by x/2 desc, x*8 desc limit 5\s
                )
                order by x*2 asc
                limit 3""")
                .ddl(null)
                .expectSize()
                .returns("x\n6\n7\n8\n");
    }

    @Test
    public void testOrderByWithAlphanumericNamedColumn() throws Exception {
        assertMemoryLeak(() -> assertQuery("""
                SELECT * FROM (
                  SELECT 456 AS "5_sum"
                  UNION ALL\s
                  SELECT 789 AS "5_sum"
                  UNION ALL\s
                  SELECT 123 AS "5_sum"
                )
                ORDER BY "5_sum\"""")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        5_sum
                        123
                        456
                        789
                        """));
    }

    @Test
    public void testOrderByOnNullConstantDoesNotLeakTreeChain() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c0 TIMESTAMP, c1 DOUBLE, c2 DOUBLE[], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (NULL, 1.0, ARRAY[1.0,2.0], '2024-01-01T00:00:00.000000Z')");

            // Multi-column ORDER BY where one key resolves to a NULL constant.
            // RecordComparatorCompiler throws SqlException("column type is not
            // supported for order by: NULL") part-way through factory
            // construction, and a partially built sort factory leaks its
            // NATIVE_TREE_CHAIN allocation.
            try {
                execute("SELECT null AS e0, c2 AS e1, (c1)::INT AS e2 FROM t ORDER BY 3, 1");
                Assert.fail("expected SqlException");
            } catch (SqlException expected) {
                TestUtils.assertContains(expected.getFlyweightMessage(), "column type is not supported for order by: NULL");
            }
        });
    }

    @Test
    public void testOrderByPositionAfterCountDistinctRewrite() throws Exception {
        // SqlOptimiser.rewriteCountDistinct lifts the count_distinct argument
        // into an inner GROUP BY model whose alias comes from the AST token.
        // For a CAST argument that token is "cast", which used to leak into
        // positional ORDER BY resolution because rewriteOrderByPosition picked
        // the inner GROUP BY's bottom-up columns. The optimiser now resolves
        // positional refs against the outermost SELECT projection.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c1 BOOLEAN)");
            execute("INSERT INTO t VALUES (true)");
            assertQuery("SELECT count_distinct('M'::CHAR) AS a0 FROM t ORDER BY 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a0\n1\n");
        });
    }

    @Test
    public void testOrderByWithAmbiguousColumnOrdering() throws Exception {
        assertQuery("""
                SELECT * FROM (
                  SELECT 456 AS "5", 123 AS "1"
                  UNION ALL\s
                  SELECT 789 AS "5",  456 AS "1"
                  UNION ALL\s
                  SELECT 123 AS "5",  999 AS "1"
                )
                ORDER BY 1""")
                .expectSize()
                .returns("""
                        5\t1
                        123\t999
                        456\t123
                        789\t456
                        """);
    }
}
