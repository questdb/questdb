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

package io.questdb.test.griffin.engine.functions.eq;


import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import org.junit.Test;

public class EqSymFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testLargeSymbolTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol(4000,1,7,3) a, rnd_symbol(4000,1,7,3) b from long_sequence(5000))");
            assertQuery("select count() from x where a = b")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            288
                            """);
        });
    }

    @Test
    public void testNonStaticSymbolsFromSameTableCompareByValue() throws Exception {
        // A UNION re-symbolises its columns through one dictionary that is not static, so = falls
        // back to the text comparison. Both sides then resolve through the same table and must
        // read distinct flyweights, otherwise the second read clobbers the first and every pair
        // compares equal.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k SYMBOL, a SYMBOL)");
            execute("CREATE TABLE y (k SYMBOL, a SYMBOL)");
            execute("""
                    INSERT INTO x VALUES
                    ('k1', 'a1'),
                    ('k2', 'a2'),
                    ('k3', 'a3')
                    """);
            execute("""
                    INSERT INTO y VALUES
                    ('k1', 'a1'),
                    ('k2', 'a4'),
                    ('k3', NULL)
                    """);
            assertQuery("""
                    SELECT k, f, l FROM (
                        SELECT k, first(a) f, last(a) l
                        FROM (SELECT k, a FROM x UNION ALL SELECT k, a FROM y)
                    ) WHERE f = l ORDER BY k
                    """)
                    .returns("""
                            k\tf\tl
                            k1\ta1\ta1
                            """);
            assertQuery("""
                    SELECT k, f, l FROM (
                        SELECT k, first(a) f, last(a) l
                        FROM (SELECT k, a FROM x UNION ALL SELECT k, a FROM y)
                    ) WHERE f != l ORDER BY k
                    """)
                    .returns("""
                            k\tf\tl
                            k2\ta2\ta4
                            k3\ta3\t
                            """);
        });
    }

    @Test
    public void testNullExtendedRows() throws Exception {
        // An outer join null-extends rows whose symbol table may store no NULL: b stores none,
        // and a constant-false ON clause replaces the null-extended input with an empty table.
        // NULL must still equal NULL there, in both argument orders.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (s SYMBOL, k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE b (s SYMBOL, k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO a VALUES
                        ('x', 1, '2024-01-01T00:00'),
                        (NULL, 2, '2024-01-01T01:00'),
                        ('y', 3, '2024-01-01T02:00')
                    """);
            execute("""
                    INSERT INTO b VALUES
                        ('x', 1, '2024-01-01T00:00'),
                        ('z', 4, '2024-01-01T01:00')
                    """);
            final String select = "SELECT l.k lk, r.k rk, l.s = l.s ll, r.s = r.s rr, l.s = r.s lr, r.s = l.s rl, "
                    + "l.s != l.s nll, r.s != r.s nrr, l.s != r.s nlr FROM a l ";
            final String[] joins = {"LEFT JOIN", "RIGHT JOIN", "FULL JOIN"};
            final String[][] results = {
                    {
                            """
                            lk\trk\tll\trr\tlr\trl\tnll\tnrr\tnlr
                            1\t1\ttrue\ttrue\ttrue\ttrue\tfalse\tfalse\tfalse
                            2\tnull\ttrue\ttrue\ttrue\ttrue\tfalse\tfalse\tfalse
                            3\tnull\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue
                            """,
                            """
                            lk\trk\tll\trr\tlr\trl\tnll\tnrr\tnlr
                            1\tnull\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue
                            2\tnull\ttrue\ttrue\ttrue\ttrue\tfalse\tfalse\tfalse
                            3\tnull\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue
                            """
                    },
                    {
                            """
                            lk\trk\tll\trr\tlr\trl\tnll\tnrr\tnlr
                            null\t4\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue
                            1\t1\ttrue\ttrue\ttrue\ttrue\tfalse\tfalse\tfalse
                            """,
                            """
                            lk\trk\tll\trr\tlr\trl\tnll\tnrr\tnlr
                            null\t1\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue
                            null\t4\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue
                            """
                    },
                    {
                            """
                            lk\trk\tll\trr\tlr\trl\tnll\tnrr\tnlr
                            null\t4\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue
                            1\t1\ttrue\ttrue\ttrue\ttrue\tfalse\tfalse\tfalse
                            2\tnull\ttrue\ttrue\ttrue\ttrue\tfalse\tfalse\tfalse
                            3\tnull\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue
                            """,
                            """
                            lk\trk\tll\trr\tlr\trl\tnll\tnrr\tnlr
                            null\t1\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue
                            null\t4\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue
                            1\tnull\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue
                            2\tnull\ttrue\ttrue\ttrue\ttrue\tfalse\tfalse\tfalse
                            3\tnull\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue
                            """
                    }
            };
            final String[] ons = {"l.k = r.k", "l.k = r.k AND 1 = 2"};
            for (int i = 0; i < joins.length; i++) {
                for (int j = 0; j < ons.length; j++) {
                    final String sql = select + joins[i] + " b r ON " + ons[j] + " ORDER BY lk, rk";
                    // The constant-false ON clause replaces the null-extended input of LEFT and
                    // RIGHT joins with an empty table; FULL joins keep both tables.
                    final QueryAssertion assertion = assertQuery(sql);
                    if (j == 1 && i < 2) {
                        assertion.withPlanContaining("Empty table");
                    } else {
                        assertion.withPlanNotContaining("Empty table");
                    }
                    assertion.returns(results[i][j]);
                }
            }
        });
    }

    @Test
    public void testNullFromOuterJoinMatchesNull() throws Exception {
        // y.s stores no NULL, so its dictionary reports containsNullValue() == false. The outer
        // join's null record still hands out a NULL y.s for the unmatched row, and it has to
        // equal the stored NULL in x.s, in line with the STRING comparison.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, s SYMBOL)");
            execute("CREATE TABLE y (id INT, s SYMBOL)");
            execute("INSERT INTO x VALUES (1, NULL), (2, 'a'), (3, 'b'), (4, 'c')");
            execute("INSERT INTO y VALUES (2, 'a'), (3, 'c')");

            assertQuery("""
                    SELECT x.id, x.s = y.s eq, y.s = x.s eq_rev, x.s != y.s ne, x.s::STRING = y.s::STRING str_eq
                    FROM x LEFT JOIN y ON x.id = y.id
                    """)
                    .noRandomAccess()
                    .returns("""
                            id\teq\teq_rev\tne\tstr_eq
                            1\ttrue\ttrue\tfalse\ttrue
                            2\ttrue\ttrue\tfalse\ttrue
                            3\tfalse\tfalse\ttrue\tfalse
                            4\tfalse\tfalse\ttrue\tfalse
                            """);
            assertQuery("SELECT x.id FROM x LEFT JOIN y ON x.id = y.id WHERE x.s = y.s")
                    .noRandomAccess()
                    .returns("""
                            id
                            1
                            2
                            """);
        });
    }

    @Test
    public void testSmoke() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5',null) a, rnd_symbol('1','4','5',null) b from long_sequence(50))");
            assertQuery("select * from x where a = b")
                    .returns("""
                            a\tb
                            1\t1
                            \t
                            1\t1
                            \t
                            5\t5
                            \t
                            5\t5
                            \t
                            1\t1
                            """);
        });
    }
}
