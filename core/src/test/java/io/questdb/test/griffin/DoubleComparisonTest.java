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

public class DoubleComparisonTest extends AbstractCairoTest {
    @Test
    public void testEquals() throws Exception {
        execute("create table foo as (select rnd_double() a from long_sequence(100000))");
        assertQuery("""
                select count() c from (
                select a -b = 0.0125 k from (
                select a - 0.0125 b, a from foo
                )
                ) where k;""")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("""
                        c
                        100000
                        """);
    }

    @Test
    public void testGt() throws Exception {
        assertQuery("select 1 - 0.9875 > 0.0125, 100.0084375 - 99.9959375 > 0.0125, (1 + 100) - (0.9875 + 100) > 0.0125;\n")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        column\tcolumn1\tcolumn2
                        false\tfalse\tfalse
                        """);
    }

    @Test
    public void testNullConsistency() throws Exception {
        assertQuery("""
                select\s
                  a = b "1",\s
                  a = cast(null as double) "2",\s
                  cast(null as double) = cast(null as double) "3",\s
                  b = cast(null as double) "4",\s
                  a = cast(null as double) "5",\s
                  a "6", \s
                  b "7",\s
                  a >= b "8",\s
                  b >= a "9",\s
                  a >= cast(null as double) "10",\s
                  b >= cast(null as double) "11",\s
                  a = a "12",\s
                  b = b "13",\s
                from (
                  select (1.0/0.0) a, cast(null as double) b
                  from long_sequence(1)
                );""")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        1\t2\t3\t4\t5\t6\t7\t8\t9\t10\t11\t12\t13
                        true\ttrue\ttrue\ttrue\ttrue\tnull\tnull\ttrue\ttrue\ttrue\ttrue\ttrue\ttrue
                        """);
    }

    @Test
    public void testNullLongInt() throws Exception {
        assertQuery("""
                select a >= b, b >= a
                from (
                  select cast(null as long) a, cast(null as int) b
                  from long_sequence(1)
                )""")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        column\tcolumn1
                        true\ttrue
                        """);
    }
}
