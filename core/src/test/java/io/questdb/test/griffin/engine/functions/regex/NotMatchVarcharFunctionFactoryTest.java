/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin.engine.functions.regex;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class NotMatchVarcharFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testNullRegex() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select rnd_varchar() name from long_sequence(2000))");
            try {
                assertException("select * from x where name !~ null");
            } catch (SqlException e) {
                Assert.assertEquals(30, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "NULL regex");
            }
        });
    }

    @Test
    public void testRegexSyntaxError() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select rnd_varchar() name from long_sequence(2000))");
            try {
                assertException("select * from x where name !~ 'XJ**'");
            } catch (SqlException e) {
                Assert.assertEquals(34, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "Dangling meta");
            }
        });
    }

    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "name\n" +
                    "\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\n" +
                    "\uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46\n" +
                    "\u0093ً\uDAF5\uDE17qRӽ-\uDBED\uDC98\uDA30\uDEE0\n" +
                    "W씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3gX夺\n" +
                    "ץ;윦\u0382宏㔸\n" +
                    "\uDAEE\uDC4FƑ䈔b\n" +
                    "4h볱9\n" +
                    "\uDB0D\uDF729\uD8FA\uDE36\u05EC\n" +
                    "혾{>\n";
            ddl("create table x as (select rnd_varchar() name from long_sequence(20))");
            assertSql(
                    expected,
                    "select * from x where name !~ '[ABCDEFGHIJKLMN]'"
            );
        });
    }
}
