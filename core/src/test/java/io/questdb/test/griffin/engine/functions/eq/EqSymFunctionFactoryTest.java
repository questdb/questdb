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
