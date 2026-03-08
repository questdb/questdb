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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class LPadRPadFunctionsTest extends AbstractCairoTest {

    @Test
    public void testAscii() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_varchar('abc','def','ghi') vc from long_sequence(5))");
            assertSql(
                    "lpad\trpad\n" +
                            "  abc\tabc  \n" +
                            "  abc\tabc  \n" +
                            "  def\tdef  \n" +
                            "  ghi\tghi  \n" +
                            "  ghi\tghi  \n",
                    "select lpad(vc, 5), rpad(vc, 5) from x order by vc"
            );
        });
    }

    @Test
    public void testAsciiFill() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_varchar('abc','def','ghi') vc from long_sequence(5))");
            assertSql(
                    "lpad\trpad\n" +
                            "..abc\tabc..\n" +
                            "..abc\tabc..\n" +
                            "..def\tdef..\n" +
                            "..ghi\tghi..\n" +
                            "..ghi\tghi..\n",
                    "select lpad(vc, 5, '.'::varchar), rpad(vc, 5, '.'::varchar) from x order by vc"
            );
        });
    }

    @Test
    public void testAsciiTrim() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_varchar('abc','def','ghi') vc from long_sequence(5))");
            final String expected = "lpad\trpad\n" +
                    "ab\tbc\n" +
                    "ab\tbc\n" +
                    "de\tef\n" +
                    "gh\thi\n" +
                    "gh\thi\n";

            assertSql(
                    expected,
                    "select lpad(vc, 2), rpad(vc, 2) from x order by vc"
            );

            assertSql(
                    expected,
                    "select lpad(vc, 2, '.'::varchar), rpad(vc, 2, '.'::varchar) from x order by vc"
            );
        });
    }

    @Test
    public void testCheckMaxBuffer() throws Exception {
        final int maxLength = configuration.getStrFunctionMaxBufferLength();
        assertException(
                "select rpad('w3r'::varchar, " + (maxLength + 1) + ")",
                0,
                "breached memory limit set for rpad(ØI) [maxLength=1048576, requiredLength=1048577]"
        );

        assertException(
                "select lpad('w3r'::varchar, " + (maxLength + 1) + ")",
                0,
                "breached memory limit set for lpad(ØI) [maxLength=1048576, requiredLength=1048577]"
        );

        assertException(
                "select rpad('w3r'::varchar, " + (maxLength + 1) + ", 'esource'::varchar)",
                0,
                "breached memory limit set for rpad(ØIØ) [maxLength=1048576, requiredLength=1048577]"
        );

        assertException(
                "select lpad('w3r'::varchar, " + (maxLength + 1) + ", 'esource'::varchar)",
                0,
                "breached memory limit set for lpad(ØIØ) [maxLength=1048576, requiredLength=1048577]"
        );
    }

    @Test
    public void testFuncFuncGenericCase() throws SqlException {
        execute("create table x as (select rnd_varchar('x', 'y', 'z') str, rnd_varchar('abc','def','ghi') fill from long_sequence(5))");
        assertSql(
                "lpad\trpad\n" +
                        "abcabcx\txabcabc\n" +
                        "ghighiy\tyghighi\n" +
                        "ghighiz\tzghighi\n" +
                        "defdefz\tzdefdef\n" +
                        "defdefx\txdefdef\n",
                "select lpad(str, 7, fill), rpad(str, 7, fill) from x"
        );
    }

    @Test
    public void testPadNulls() throws SqlException {
        execute("create table x as (select null::varchar as vc from long_sequence(10))");
        assertSql(
                "count\n10\n",
                "select count (*) from (select lpad(vc, 20, '.'::varchar), rpad(vc, 20, '.'::varchar) from x order by vc) where lpad = null and rpad = null"
        );
        assertSql(
                "count\n10\n",
                "select count (*) from (select lpad(vc, 20), rpad(vc, 20) from x order by vc) where lpad = null and rpad = null"
        );
    }

    @Test
    public void testSimple() throws SqlException {
        configuration.getStrFunctionMaxBufferLength();
        assertSql("rpad\nw3resou\n", "select rpad('w3r'::varchar, 7, 'esource'::varchar)");
    }

    @Test
    public void testStrIsConst() throws SqlException {
        execute("create table x as (select rnd_varchar('abc','def','ghi') fill from long_sequence(5))");
        assertSql(
                "lpad\trpad\n" +
                        "abcabcconst\tconstabcabc\n" +
                        "abcabcconst\tconstabcabc\n" +
                        "defdefconst\tconstdefdef\n" +
                        "ghighiconst\tconstghighi\n" +
                        "ghighiconst\tconstghighi\n",
                "select lpad('const', 11, fill), rpad('const', 11, fill) from x"
        );
    }

    @Test
    public void testUtf8() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_varchar('ганьба','слава','добрий','вечір') vc from long_sequence(6))");
            assertSql(
                    "lpad\trpad\n" +
                            "     вечір\tвечір     \n" +
                            "     вечір\tвечір     \n" +
                            "    ганьба\tганьба    \n" +
                            "    добрий\tдобрий    \n" +
                            "     слава\tслава     \n" +
                            "     слава\tслава     \n",
                    "select lpad(vc, 10), rpad(vc, 10) from x order by vc"
            );
        });
    }

    @Test
    public void testUtf8Fill() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_varchar('ганьба','слава','добрий','вечір') vc from long_sequence(6))");
            assertSql(
                    "lpad\trpad\n" +
                            ".....вечір\tвечір.....\n" +
                            ".....вечір\tвечір.....\n" +
                            "....ганьба\tганьба....\n" +
                            "....добрий\tдобрий....\n" +
                            ".....слава\tслава.....\n" +
                            ".....слава\tслава.....\n",
                    "select lpad(vc, 10, '.'::varchar), rpad(vc, 10, '.'::varchar) from x order by vc"
            );
        });
    }

    @Test
    public void testUtf8Random() throws SqlException {
        execute("create table x as (select rnd_varchar(1, 40, 0) vc1, rnd_varchar(1, 4, 0) vc2 from long_sequence(100))");
        assertSql(
                "count\n" +
                        "100\n",
                "select count (*) from (select length(lpad(vc1, 20)) ll, length(rpad(vc2, 20)) lr from x) where ll = 20 and lr = 20"
        );
        // test A/B case
        assertSql(
                "count\n" +
                        "100\n",
                "select count (*) from x where lpad(vc1, 20) = lpad(vc1, 20)"
        );

        assertSql(
                "count\n" +
                        "100\n",
                "select count (*) from x where rpad(vc2, 20) = rpad(vc2, 20)"
        );
    }

    @Test
    public void testUtf8RandomFill() throws SqlException {
        execute("create table x as (select rnd_varchar(1, 40, 0) vc1, rnd_varchar(1, 4, 0) vc2 from long_sequence(100))");
        assertSql(
                "count\n" +
                        "100\n",
                "select count (*) from (select length(lpad(vc1, 20, '.'::varchar)) ll, length(rpad(vc2, 20, '.'::varchar)) lr from x order by vc1, vc2) where ll = 20 and lr = 20"
        );

        assertSql(
                "count\n" +
                        "100\n",
                "select count (*) from x where lpad(vc1, 20, '.'::varchar) = lpad(vc1, 20, '.'::varchar)"
        );

        assertSql(
                "count\n" +
                        "100\n",
                "select count (*) from x where rpad(vc2, 20, '.'::varchar) = rpad(vc2, 20, '.'::varchar)"
        );
    }

    @Test
    public void testUtf8Trim() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_varchar('ганьба','слава','добрий','вечір') vc from long_sequence(6))");
            final String expected = "lpad\trpad\n" +
                    "вечі\tечір\n" +
                    "вечі\tечір\n" +
                    "гань\tньба\n" +
                    "добр\tбрий\n" +
                    "слав\tлава\n" +
                    "слав\tлава\n";
            assertSql(
                    expected,
                    "select lpad(vc, 4), rpad(vc, 4) from x order by vc"
            );
            assertSql(
                    expected,
                    "select lpad(vc, 4, '.'::varchar), rpad(vc, 4, '.'::varchar) from x order by vc"
            );
        });
    }
}
