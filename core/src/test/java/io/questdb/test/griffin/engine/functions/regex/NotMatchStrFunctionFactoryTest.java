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

package io.questdb.test.griffin.engine.functions.regex;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class NotMatchStrFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testNullRegex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_str() name from long_sequence(2000))");
            assertQuery(
                    "name\n",
                    "select * from x where name !~ null",
                    false,
                    true
            );
        });
    }

    @Test
    public void testRegexSyntaxError() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_str() name from long_sequence(2000))");
            try {
                assertExceptionNoLeakCheck("select * from x where name !~ 'XJ**'");
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
                    "XYPO\n" +
                    "XTP\n" +
                    "PZP\n" +
                    "WZOO\n" +
                    "SYY\n" +
                    "WVY\n" +
                    "TRVYQ\n" +
                    "RSX\n" +
                    "YRZO\n" +
                    "WRQ\n" +
                    "QSW\n" +
                    "PZWY\n" +
                    "WOZZV\n" +
                    "YRS\n" +
                    "PQU\n" +
                    "SUXQSWVR\n" +
                    "ROVRQZV\n" +
                    "WPSU\n" +
                    "QPW\n" +
                    "OQO\n" +
                    "WZWX\n" +
                    "PZPW\n" +
                    "QZY\n" +
                    "ZQR\n" +
                    "ZPXR\n" +
                    "TYVU\n" +
                    "VOW\n" +
                    "WYX\n" +
                    "ZWTO\n" +
                    "VTR\n" +
                    "QXXY\n" +
                    "UUWV\n" +
                    "PYW\n" +
                    "YOP\n" +
                    "YVXZ\n" +
                    "SYYQ\n" +
                    "TVX\n" +
                    "UQRVV\n" +
                    "USUT\n" +
                    "OQVS\n" +
                    "SSSR\n" +
                    "WZV\n" +
                    "PZX\n" +
                    "ZOYYO\n" +
                    "SXY\n" +
                    "XZU\n" +
                    "YPX\n" +
                    "ROU\n" +
                    "OPY\n" +
                    "YPR\n";
            execute("create table x as (select rnd_str() name from long_sequence(2000))");
            assertSql(
                    expected,
                    "select * from x where name !~ '[ABCDEFGHIJKLMN]'"
            );
        });
    }
}
