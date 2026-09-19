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
