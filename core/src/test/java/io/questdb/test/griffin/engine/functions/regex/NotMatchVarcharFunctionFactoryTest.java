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
            execute("create table x as (select rnd_varchar() name from long_sequence(2000))");
            assertQuery("select * from x where name !~ null")
                    .noLeakCheck()
                    .expectSize()
                    .returns("name\n");
        });
    }

    @Test
    public void testRegexSyntaxError() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_varchar() name from long_sequence(2000))");
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
            final String expected = """
                    name
                    8#3TsZ
                    zV衞͛Ԉ龘и\uDA89\uDFA4~
                    \uDBAE\uDD12ɜ|
                    \uDB59\uDF3B룒jᷚ
                    p-鳓w
                    h\uDAF5\uDE17qRӽ-
                    Ǆ Ԡ阷l싒8쮠
                    kɷ씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3
                    \uD908\uDECBŗ\uDB47\uDD9C\uDA96\uDF8F㔸
                    91g>
                    h볱9
                    """;
            execute("create table x as (select rnd_varchar() name from long_sequence(20))");
            assertQuery("select * from x where name !~ '[ABCDEFGHIJKLMN]'")
                    .noLeakCheck()
                    .returns(expected);
        });
    }
}
