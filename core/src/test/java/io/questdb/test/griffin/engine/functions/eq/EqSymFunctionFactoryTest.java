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
