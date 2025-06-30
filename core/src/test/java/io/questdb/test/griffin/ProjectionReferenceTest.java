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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ProjectionReferenceTest extends AbstractCairoTest {

    @Test
    public void testAsofJoinSimple() throws Exception {
        execute("create table events (symbol string, value int, ts timestamp) timestamp(ts)");
        execute("create table quotes (symbol string, quote int, ts timestamp) timestamp(ts)");

        execute("insert into events values ('A', 100, '2025-01-01T10:00:00.000Z'), ('A', 200, '2025-01-01T10:05:00.000Z')");
        execute("insert into quotes values ('A', 10, '2025-01-01T09:59:00.000Z'), ('A', 20, '2025-01-01T10:03:00.000Z')");

        // Simple ASOF JOIN without projection references
        assertQuery(
                "symbol\tvalue\tquote\tsum\n" +
                        "A\t100\t10\t110\n" +
                        "A\t200\t20\t220\n",
                "select e.symbol, e.value, q.quote, e.value + q.quote as sum " +
                        "from events e asof join quotes q on e.symbol = q.symbol",
                false,
                true
        );
    }

    @Test
    public void testBindingVars() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong(0, 1);

            assertQuery(
                    "b\tinc\n" +
                            "1\t2\n" +
                            "1\t2\n" +
                            "1\t2\n",
                    "select $1 as b, b + 1 as inc from long_sequence(3)",
                    true,
                    true
            );

            // we can use a projected column insude an expression
            assertQuery(
                    "b\tinc\n" +
                            "2\t3\n" +
                            "3\t4\n" +
                            "4\t5\n",
                    "select $1 + x as b, b + 1 as inc from long_sequence(3)",
                    true,
                    true
            );

            // we prioritise base column over projection
            assertQuery(
                    "x\tx_orig\n" +
                            "1\t1\n" +
                            "1\t2\n" +
                            "1\t3\n",
                    "select $1 as x, x as x_orig from long_sequence(3)",
                    true,
                    true
            );

            assertQuery(
                    "x\tx_orig\n" +
                            "2\t1\n" +
                            "3\t2\n" +
                            "4\t3\n",
                    "select $1 + x as x, x as x_orig from long_sequence(3)",
                    true,
                    true
            );
        });
    }

    @Test
    public void testInnerJoinSimple() throws Exception {
        execute("create table t1 (id int, val int)");
        execute("create table t2 (id int, val int)");
        execute("insert into t1 values (1, 10), (2, 20)");
        execute("insert into t2 values (1, 100), (2, 200)");

        // Simple join without projection references to ensure JOIN works
        assertQuery(
                "id\tval1\tval2\tsum\n" +
                        "1\t10\t100\t110\n" +
                        "2\t20\t200\t220\n",
                "select t1.id, t1.val as val1, t2.val as val2, t1.val + t2.val as sum " +
                        "from t1 inner join t2 on t1.id = t2.id",
                false,
                true
        );
    }

    @Test
    public void testJoinWithProjectionReference() throws Exception {
        execute("create table orders (id int, amount int)");
        execute("create table customers (id int, name string)");
        execute("insert into orders values (1, 100), (2, 200)");
        execute("insert into customers values (1, 'Alice'), (2, 'Bob')");

        assertQuery(
                "order_id\tcustomer_name\tamount\ttax\ttotal\n" +
                        "1\tAlice\t100\t10.0\t110.0\n" +
                        "2\tBob\t200\t20.0\t220.0\n",
                "select" +
                        " o.id as order_id," +
                        " c.name as customer_name," +
                        " o.amount," +
                        " o.amount * 0.1 as tax," +
                        " o.amount + tax as total" +
                        " from orders o join customers c on o.id = c.id",
                false,
                true
        );
    }

    @Test
    public void testMultipleLevelProjections() throws Exception {
        execute("create table data (x int)");
        execute("insert into data values (1), (2), (3)");

        assertQuery(
                "x\ta\tb\tc\td\n" +
                        "1\t2\t4\t8\t16\n" +
                        "2\t3\t5\t9\t17\n" +
                        "3\t4\t6\t10\t18\n",
                "select x, x + 1 as a, a + 2 as b, b + 4 as c, c + 8 as d from data",
                true
        );
    }

    @Test
    public void testNestedSubquerySimple() throws Exception {
        execute("create table base (id int, value int)");
        execute("insert into base values (1, 10), (2, 20), (3, 30)");

        // Test projection references across subquery boundaries
        assertQuery(
                "id\tdoubled\n" +
                        "1\t20\n" +
                        "2\t40\n" +
                        "3\t60\n",
                "select id, doubled from (select id, value * 2 as doubled from base)",
                true
        );
    }

    @Test
    public void testPreferBaseColumnOverProjectionVanilla() throws Exception {
        execute("create table temp (x int)");
        execute("insert into temp values (1), (2), (3)");
        assertQuery(
                "x\tcolumn\n" +
                        "11\t-4\n" +
                        "12\t-3\n" +
                        "13\t-2\n",
                "select x + 10 x, x - 5 from temp",
                true
        );
    }

    @Test
    public void testProjectionAliasPreference() throws Exception {
        execute("create table test (a int, b int)");
        execute("insert into test values (5, 10), (15, 20)");

        // Verify that when we create an alias with the same name as a column,
        // references still use the original column (not the alias)
        assertQuery(
                "a\tb\toriginal_a\n" +
                        "15\t10\t5\n" +
                        "35\t20\t15\n",
                "select a + b as a, b, a as original_a from test",
                true
        );
    }

    @Test
    public void testProjectionInOrderBy() throws Exception {
        execute("create table items (name string, value int)");
        execute("insert into items values ('C', 30), ('A', 10), ('B', 20)");

        // Test ORDER BY with base columns
        assertQuery(
                "name\tvalue\tdoubled\n" +
                        "A\t10\t20\n" +
                        "B\t20\t40\n" +
                        "C\t30\t60\n",
                "select name, value, value * 2 as doubled from items order by value",
                true
        );
    }

    @Test
    public void testProjectionInWhereClause() throws Exception {
        execute("create table data (x int, y int)");
        execute("insert into data values (1, 10), (2, 20), (3, 30), (4, 40)");

        // Test that WHERE clause uses base columns, not projections
        assertQuery(
                "x\n" +
                        "22\n" +
                        "33\n" +
                        "44\n",
                "select x + y as x from data where x > 1",
                false
        );
    }

    @Test
    public void testProjectionWithArithmetic() throws Exception {
        execute("create table numbers (n int)");
        execute("insert into numbers values (10), (20), (30)");

        // Test that projection references work with various arithmetic operations
        assertQuery(
                "n\tdouble_n\ttriple_n\thalf_of_double\n" +
                        "10\t20\t30\t10\n" +
                        "20\t40\t60\t20\n" +
                        "30\t60\t90\t30\n",
                "select n, n * 2 as double_n, double_n + n as triple_n, double_n / 2 as half_of_double from numbers",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testProjectionWithCase() throws Exception {
        execute("create table grades (score int)");
        execute("insert into grades values (95), (85), (75), (65)");

        assertQuery(
                "score\tgrade\tpass_status\n" +
                        "95\tA\tPASS\n" +
                        "85\tB\tPASS\n" +
                        "75\tC\tPASS\n" +
                        "65\tD\tFAIL\n",
                "select score, " +
                        "case when score >= 90 then 'A' " +
                        "     when score >= 80 then 'B' " +
                        "     when score >= 70 then 'C' " +
                        "     else 'D' end as grade, " +
                        "case when grade in ('A', 'B', 'C') then 'PASS' else 'FAIL' end as pass_status " +
                        "from grades",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testSimpleProjectionChain() throws Exception {
        execute("create table data (x int)");
        execute("insert into data values (1), (2), (3)");

        // Test simple chaining: x -> a -> b
        assertQuery(
                "x\ta\tb\n" +
                        "1\t2\t4\n" +
                        "2\t3\t5\n" +
                        "3\t4\t6\n",
                "select x, x + 1 as a, a + 2 as b from data",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testUnionAll() throws Exception {
        // note: different types in union all -> it also exercises type coercion
        execute("create table temp (x int)");
        execute("create table temp2 (x long)");
        execute("insert into temp values (1), (2), (3)");
        execute("insert into temp2 values (4), (5), (6)");

        assertQuery(
                "x\tdec\n" +
                        "2\t0\n" +
                        "3\t1\n" +
                        "4\t2\n" +
                        "5\t3\n" +
                        "6\t4\n" +
                        "7\t5\n",
                "select x + 1 as x, x - 1 as dec from temp union all select x + 1 as x, x - 1 from temp2",
                null,
                null,
                false,
                true
        );
    }

    @Test
    public void testUnion_overlappingOnAllColumns() throws Exception {
        execute("create table temp (x int)");
        execute("create table temp2 (x long)");
        execute("insert into temp values (1), (2), (3)");
        execute("insert into temp2 values (2), (3), (4)");

        assertQuery(
                "x\tdec\n" +
                        "2\t0\n" +
                        "3\t1\n" +
                        "4\t2\n" +
                        "5\t3\n",
                "select x + 1 as x, x - 1 as dec from temp union select x + 1 as x, x - 1 from temp2",
                null,
                null,
                false,
                false
        );
    }

    @Test
    public void testUnion_overlappingOnProjectedColumnOnly() throws Exception {
        execute("create table temp (x int)");
        execute("create table temp2 (x long)");
        execute("insert into temp values (1), (2), (3)");
        execute("insert into temp2 values (4), (5), (6)");

        // overlapping rows with different types
        assertQuery(
                "x\tb\n" +
                        "-1\ttrue\n" +
                        "-2\ttrue\n" +
                        "-3\ttrue\n" +
                        "-4\ttrue\n" +
                        "-5\ttrue\n" +
                        "-6\ttrue\n",
                "select -x as x, x > 0 as b from temp union select -x as x, x > 0 from temp2",
                null,
                null,
                false,
                false
        );
    }

    @Test
    public void testVanilla() throws Exception {
        execute("create table tmp as" +
                " (select" +
                " rnd_double() a," +
                " timestamp_sequence('2025-06-22'::timestamp, 150099) ts" +
                " from long_sequence(10)" +
                ") timestamp(ts) partition by hour");
        assertQuery(
                "i\tcolumn\n" +
                        "1.3215555788374664\t2.3215555788374664\n" +
                        "0.4492602684994518\t1.4492602684994518\n" +
                        "0.16973928465121335\t1.1697392846512134\n" +
                        "0.59839809192369\t1.59839809192369\n" +
                        "0.4089488367575551\t1.4089488367575551\n" +
                        "1.3017188051710602\t2.30171880517106\n" +
                        "1.684682184176669\t2.684682184176669\n" +
                        "1.9712581691748525\t2.9712581691748525\n" +
                        "0.44904681712176453\t1.4490468171217645\n" +
                        "1.0187654003234814\t2.018765400323481\n",
                "select a * 2 i, i + 1 from tmp;",
                true
        );
    }

    @Test
    public void testVirtualFunctionAsColumnReference() throws Exception {
        assertSql(
                "k\tk1\n" +
                        "-1148479919\t315515119\n" +
                        "1548800834\t-727724770\n" +
                        "73575702\t-948263338\n" +
                        "1326447243\t592859672\n" +
                        "1868723707\t-847531047\n" +
                        "-1191262515\t-2041844971\n" +
                        "-1436881713\t-1575378702\n" +
                        "806715482\t1545253513\n" +
                        "1569490117\t1573662098\n" +
                        "-409854404\t339631475\n",
                "select rnd_int() + 1 k, k from long_sequence(10)"
        );
    }

    @Test
    public void testVirtualFunctionAsColumnReferencePreferBaseTable() throws Exception {
        assertSql(
                "x\tx1\n" +
                        "-1148479919\t1\n" +
                        "315515119\t2\n" +
                        "1548800834\t3\n" +
                        "-727724770\t4\n" +
                        "73575702\t5\n" +
                        "-948263338\t6\n" +
                        "1326447243\t7\n" +
                        "592859672\t8\n" +
                        "1868723707\t9\n" +
                        "-847531047\t10\n",
                "select rnd_int() + 1 x, x from long_sequence(10)"
        );
    }

    @Test
    public void testColumnAsColumnReference() throws Exception {
        assertSql(
                "k\tk1\n" +
                        "1\t1\n" +
                        "2\t2\n" +
                        "3\t3\n" +
                        "4\t4\n" +
                        "5\t5\n" +
                        "6\t6\n" +
                        "7\t7\n" +
                        "8\t8\n" +
                        "9\t9\n" +
                        "10\t10\n",
                "select x k, k from long_sequence(10)"
        );
    }

    @Test
    public void testColumnAsColumnReferencePreferBaseTable() throws Exception {
        assertSql(
                "x\tx1\n" +
                        "1\t1\n" +
                        "2\t2\n" +
                        "3\t3\n" +
                        "4\t4\n" +
                        "5\t5\n" +
                        "6\t6\n" +
                        "7\t7\n" +
                        "8\t8\n" +
                        "9\t9\n" +
                        "10\t10\n",
                "select a x, x from (select x a, x b, x from long_sequence(10))"
        );
    }
}