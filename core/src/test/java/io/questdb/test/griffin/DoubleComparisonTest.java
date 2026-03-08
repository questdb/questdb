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

package io.questdb.test.griffin;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class DoubleComparisonTest extends AbstractCairoTest {
    @Test
    public void testEquals() throws SqlException {
        execute("create table foo as (select rnd_double() a from long_sequence(100000))");
        assertSql(
                "c\n" +
                        "100000\n",
                "select count() c from (\n" +
                        "select a -b = 0.0125 k from (\n" +
                        "select a - 0.0125 b, a from foo\n" +
                        ")\n" +
                        ") where k;"
        );
    }

    @Test
    public void testGt() throws SqlException {
        assertSql(
                "column\tcolumn1\tcolumn2\n" +
                        "false\tfalse\tfalse\n",
                "select 1 - 0.9875 > 0.0125, 100.0084375 - 99.9959375 > 0.0125, (1 + 100) - (0.9875 + 100) > 0.0125;\n"
        );
    }

    @Test
    public void testNullConsistency() throws SqlException {
        assertSql(
                "1\t2\t3\t4\t5\t6\t7\t8\t9\t10\t11\t12\t13\n" +
                        "true\ttrue\ttrue\ttrue\ttrue\tnull\tnull\ttrue\ttrue\ttrue\ttrue\ttrue\ttrue\n",
                "select \n" +
                        "  a = b \"1\", \n" +
                        "  a = cast(null as double) \"2\", \n" +
                        "  cast(null as double) = cast(null as double) \"3\", \n" +
                        "  b = cast(null as double) \"4\", \n" +
                        "  a = cast(null as double) \"5\", \n" +
                        "  a \"6\",  \n" +
                        "  b \"7\", \n" +
                        "  a >= b \"8\", \n" +
                        "  b >= a \"9\", \n" +
                        "  a >= cast(null as double) \"10\", \n" +
                        "  b >= cast(null as double) \"11\", \n" +
                        "  a = a \"12\", \n" +
                        "  b = b \"13\", \n" +
                        "from (\n" +
                        "  select (1.0/0.0) a, cast(null as double) b\n" +
                        "  from long_sequence(1)\n" +
                        ");"
        );
    }

    @Test
    public void testNullLongInt() throws SqlException {
        assertSql(
                "column\tcolumn1\n" +
                        "true\ttrue\n",
                "select a >= b, b >= a\n" +
                        "from (\n" +
                        "  select cast(null as long) a, cast(null as int) b\n" +
                        "  from long_sequence(1)\n" +
                        ")"
        );
    }
}
