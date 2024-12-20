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
import org.junit.Ignore;
import org.junit.Test;

public class OrderByExpressionTest extends AbstractCairoTest {

    @Test
    public void testOrderByBinaryFails() throws Exception {
        assertException(
                "select b from (select rnd_bin(10, 20, 2) b from long_sequence(10)) order by b desc",
                76,
                "unsupported column type: BINARY"
        );
    }

    @Test
    public void testOrderByColumnInJoinedSubquery() throws Exception {
        assertQuery(
                "x\toth\n" +
                        "1\t100\n" +
                        "1\t81\n" +
                        "1\t64\n",
                "select * from \n" +
                        "(\n" +
                        "  selecT x from long_sequence(10) \n" +
                        ")\n" +
                        "cross join \n" +
                        "(\n" +
                        "  select * from \n" +
                        "  (\n" +
                        "    selecT x*x as oth from long_sequence(10) order by x desc limit 5 \n" +
                        "  )\n" +
                        ")\n" +
                        "order by x*2 asc\n" +
                        "limit 3",
                null,
                null,
                true,
                false
        );
    }

    // fails with duplicate column : column because alias created for 'x*x' clashes with one created for x+rnd_int(1,10,0)*0
    // TODO: test with order by x*2 in outer query
    @Test
    @Ignore
    public void testOrderByExpressionInJoinedSubquery() throws Exception {
        assertQuery(
                "x\tcolumn\tcolumn1\n" +
                        "1\t100\t50\n" +
                        "1\t81\t45\n" +
                        "1\t64\t40\n",
                "select * from \n" +
                        "(\n" +
                        "  select x from long_sequence(10) \n" +
                        ")\n" +
                        "cross join \n" +
                        "(\n" +
                        "    select x*x,5*x from long_sequence(10) order by x+rnd_int(1,10,0)*0 desc limit 5 \n" +
                        ")\n" +
                        "order by x*2  asc\n" +
                        "limit 3",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByExpressionInNestedQuery() throws Exception {
        assertQuery(
                "x\n6\n7\n8\n",
                "select * from \n" +
                        "(\n" +
                        "  select x from long_sequence(10) order by x/2 desc limit 5 \n" +
                        ")\n" +
                        "order by x*2 asc\n" +
                        "limit 3",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByExpressionWhenColumnHasAliasInJoinedSubquery() throws Exception {
        assertQuery(
                "x\text\n1\t100\n1\t81\n1\t64\n",
                "select * from \n" +
                        "(\n" +
                        "  select x from long_sequence(10) \n" +
                        ")\n" +
                        "cross join \n" +
                        "(\n" +
                        "    select x*x as ext from long_sequence(10) order by x+rnd_int(1,10,0)*0 desc limit 5 \n" +
                        ")\n" +
                        "order by x*2 asc, ext desc\n" +
                        "limit 3",
                null,
                null,
                true,
                false
        );
    }

    @Test
    public void testOrderByExpressionWithDuplicatesMaintainsOriginalOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab as (select x x, x%2 y from long_sequence(10))");

            assertQuery(
                    "x\ty\n" +
                            "1\t1\n" +
                            "2\t0\n" +
                            "3\t1\n" +
                            "4\t0\n" +
                            "5\t1\n" +
                            "6\t0\n" +
                            "7\t1\n" +
                            "8\t0\n" +
                            "9\t1\n" +
                            "10\t0\n",
                    "select * from tab order by x/x",
                    null,
                    true,
                    true
            );

            assertQuery(
                    "x\ty\n" +
                            "2\t0\n" +
                            "4\t0\n" +
                            "6\t0\n" +
                            "8\t0\n" +
                            "10\t0\n" +
                            "1\t1\n" +
                            "3\t1\n" +
                            "5\t1\n" +
                            "7\t1\n" +
                            "9\t1\n",
                    "select * from tab order by y, x",
                    null,
                    true,
                    true
            );

            assertQuery(
                    "x\ty\n" +
                            "2\t0\n" +
                            "4\t0\n" +
                            "6\t0\n" +
                            "8\t0\n" +
                            "10\t0\n" +
                            "1\t1\n" +
                            "3\t1\n" +
                            "5\t1\n" +
                            "7\t1\n" +
                            "9\t1\n",
                    "select * from (select t2.* from tab t1 cross join tab t2 limit 10) order by y",
                    null,
                    true,
                    true
            );

            assertQuery(
                    "x\ty\n" +
                            "1\t1\n" +
                            "2\t0\n" +
                            "3\t1\n" +
                            "4\t0\n" +
                            "5\t1\n" +
                            "6\t0\n" +
                            "7\t1\n" +
                            "8\t0\n" +
                            "9\t1\n" +
                            "10\t0\n",
                    "select * from (select t2.* from tab t1 cross join tab t2 limit 10) order by x/x",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testOrderByExpressionWithFunctionCallInNestedQuery() throws Exception {
        assertQuery(
                "x\n6\n7\n8\n",
                "select * from \n" +
                        "(\n" +
                        "    select x from long_sequence(10) order by x+rnd_int(1,10,0)*0 desc limit 5 \n" +
                        ")\n" +
                        "order by x*2 asc\n" +
                        "limit 3",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByExpressionWithFunctionCallInWithClause() throws Exception {
        assertQuery(
                "x\n6\n7\n8\n",
                "with q as (select x from long_sequence(10) order by x+rnd_int(1,10,0)*0 desc limit 5 ) \n" +
                        "select * from q\n" +
                        "order by x*2 asc\n" +
                        "limit 3",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByIntervalFails() throws Exception {
        assertException(
                "select i from (" +
                        "  (select interval(100000,200000) i) " +
                        "  union all " +
                        "  (select interval(100000,200000) i) " +
                        "  union all " +
                        "  (select null::interval i)" +
                        ") " +
                        "order by i desc",
                151,
                "unsupported column type: INTERVAL"
        );
    }

    @Test
    public void testOrderByTwoColumnsInJoin() throws Exception {
        assertQuery(
                "id\ts1\ts2\n" +
                        "42\tfoo1\tbar1\n" +
                        "42\tfoo1\tbar2\n" +
                        "42\tfoo1\tbar2\n" +
                        "42\tfoo1\tbar2\n" +
                        "42\tfoo2\tbar1\n" +
                        "42\tfoo2\tbar2\n" +
                        "42\tfoo2\tbar3\n" +
                        "42\tfoo2\tbar3\n" +
                        "42\tfoo3\tbar2\n" +
                        "42\tfoo3\tbar3\n",
                "select * " +
                        "from (" +
                        "  select b.*" +
                        "  from (select 42 id) a " +
                        "  left join (x union all (select 0 id, 'foo0' s1, 'bar0')) b on a.id = b.id" +
                        ")" +
                        "order by s1, s2",
                "create table x as (select 42 id, rnd_str('foo1','foo2','foo3') s1, rnd_str('bar1','bar2','bar3') s2 from long_sequence(10))",
                null,
                true,
                false
        );
    }

    @Test
    public void testOrderByTwoExpressions() throws Exception {
        assertQuery(
                "x\n10\n9\n8\n7\n6\n",
                "select x from long_sequence(10) order by x/100, x*x desc  limit 5",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByTwoExpressionsInNestedQuery() throws Exception {
        assertQuery(
                "x\n6\n7\n8\n",
                "select * from \n" +
                        "(\n" +
                        "  select x from long_sequence(10) order by x/2 desc, x*8 desc limit 5 \n" +
                        ")\n" +
                        "order by x*2 asc\n" +
                        "limit 3",
                null,
                null,
                true,
                true
        );
    }
}
