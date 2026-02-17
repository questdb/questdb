/*******************************************************************************
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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class AvgLongVecGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAddColumn() throws Exception {
        // fix page frame size, because it affects AVG accuracy

        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 10_000);

        assertQuery(
                """
                        avg
                        5261.376146789
                        """,
                "select round(avg(f),9) avg from tab",
                "create table tab as (select rnd_int(-55, 9009, 2) f from long_sequence(131))",
                null,
                "alter table tab add column b long",
                """
                        avg
                        5261.376146789
                        """,
                false,
                true,
                false
        );

        assertQuery(
                """
                        avg\tavg2
                        14.792007\t52790.018932
                        """,
                "select round(avg(f),6) avg, round(avg(b),6) avg2 from tab",
                "insert into tab select rnd_int(2, 10, 2), rnd_long(16772, 88965, 4) from long_sequence(78057)",
                null,
                false,
                true
        );
    }

    @Test
    public void testAllNullThenOne() throws Exception {
        assertQuery(
                """
                        avg
                        null
                        """,
                "select avg(f) from tab",
                "create table tab as (select cast(null as long) f from long_sequence(33))",
                null,
                "insert into tab select 123L from long_sequence(1)",
                """
                        avg
                        123.0
                        """,
                false,
                true,
                false
        );
    }

    @Test
    public void testAvgLongOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as(select 21474836475L * x as x, rnd_symbol('a', 'b', 'c') sym from long_sequence(1000000));");
            String expected = """
                    sym\tavg
                    a\t1.0731625369352276E16
                    b\t1.0731385513028126E16
                    c\t1.0749264817744848E16
                    """;


            assertSql(expected, "select sym, avg(cast(x as double)) from test order by sym");
            assertSql(expected, "select sym, avg(x) from test where x > 0 order by sym");

            final String diffExpected = """
                    sym\tcolumn
                    a\ttrue
                    b\ttrue
                    c\ttrue
                    """;

            // Here 6000 is a hack.
            // Difference between avg(double) and avg(long) depends on column values range and rows count.
            assertSql(diffExpected, "with a as (select sym, avg(x) from test), b as (select sym, avg(x) from test where x > 0) " +
                    "select a.sym, b.avg-a.avg < 6000 from a join b on(sym) order by sym");
        });
    }

    @Test
    public void testSimple() throws Exception {
        // fix page frame size, because it affects AVG accuracy
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 10_000);

        assertQuery(
                """
                        avg
                        4289.100917431193
                        """,
                "select avg(f) from tab",
                "create table tab as (select rnd_long(-55, 9009, 2) f from long_sequence(131))",
                null,
                false,
                true
        );
    }
}