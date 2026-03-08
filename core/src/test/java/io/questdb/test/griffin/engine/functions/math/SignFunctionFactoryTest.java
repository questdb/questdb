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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SignFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testByteShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab ( b byte )");
            execute("insert into tab values (0), " +
                    "(1), (127), " +
                    "(-1), (-128)");
            assertSql("b\tsign\n" +
                            "0\t0\n" +
                            "1\t1\n" +
                            "127\t1\n" +
                            "-1\t-1\n" +
                            "-128\t-1\n",
                    "select b, sign(b) from tab");
        });
    }

    @Test
    public void testSignDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab ( d double )");
            execute("insert into tab values (0.0), (-0.0), " +
                    "(2.2250738585072014E-308), (1.0), (1.7976931348623157E308), ('Infinity'::double), " +
                    "(-2.2250738585072014E-308), (-1.0), (-1.7976931348623157E308), ('-Infinity'::double)," +
                    "(null) ");
            assertSql("d\tsign\n" +
                            "0.0\t0.0\n" +
                            "-0.0\t0.0\n" +
                            "2.2250738585072014E-308\t1.0\n" +
                            "1.0\t1.0\n" +
                            "1.7976931348623157E308\t1.0\n" +
                            "null\tnull\n" +
                            "-2.2250738585072014E-308\t-1.0\n" +
                            "-1.0\t-1.0\n" +
                            "-1.7976931348623157E308\t-1.0\n" +
                            "null\tnull\n" +
                            "null\tnull\n",
                    "select d, sign(d) from tab");
        });
    }

    @Test
    public void testSignFloat() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab ( f float )");
            execute("insert into tab values (0.0), (-0.0), " +
                    "(1.4E-45F), (1.0), (3.4028235E38F), (cast('Infinity' as float)), " +
                    "(-1.4E-45F), (-1.0), (-3.4028235E38F), (cast('-Infinity' as float))," +
                    "(null) ");
            assertSql("f\tsign\n" +
                            "0.0\t0.0\n" +
                            "-0.0\t-0.0\n" +
                            "1.4E-45\t1.0\n" +
                            "1.0\t1.0\n" +
                            "3.4028235E38\t1.0\n" +
                            "null\tnull\n" +
                            "-1.4E-45\t-1.0\n" +
                            "-1.0\t-1.0\n" +
                            "-3.4028235E38\t-1.0\n" +
                            "null\tnull\n" +
                            "null\tnull\n",
                    "select f, sign(f) from tab");
        });
    }

    @Test
    public void testSignInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab ( i int )");
            execute("insert into tab values (0), " +
                    "(1), (2147483647), " +
                    "(-1), (-2147483647)," +
                    "(null) ");
            assertSql("i\tsign\n" +
                            "0\t0\n" +
                            "1\t1\n" +
                            "2147483647\t1\n" +
                            "-1\t-1\n" +
                            "-2147483647\t-1\n" +
                            "null\tnull\n",
                    "select i, sign(i) from tab");
        });
    }

    @Test
    public void testSignLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab ( l long )");
            execute("insert into tab values (0L), " +
                    "(1L), (9223372036854775807L), " +
                    "(-1L), (-9223372036854775807L)," +
                    "(null) ");
            assertSql("l\tsign\n" +
                            "0\t0\n" +
                            "1\t1\n" +
                            "9223372036854775807\t1\n" +
                            "-1\t-1\n" +
                            "-9223372036854775807\t-1\n" +
                            "null\tnull\n",
                    "select l, sign(l) from tab");
        });
    }

    @Test
    public void testSignShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab ( s short )");
            execute("insert into tab values (0), " +
                    "(1), (32767), " +
                    "(-1), (-32768)");
            assertSql("s\tsign\n" +
                            "0\t0\n" +
                            "1\t1\n" +
                            "32767\t1\n" +
                            "-1\t-1\n" +
                            "-32768\t-1\n",
                    "select s, sign(s) from tab");
        });
    }
}
