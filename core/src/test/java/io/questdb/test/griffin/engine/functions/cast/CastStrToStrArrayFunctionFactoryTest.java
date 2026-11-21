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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CastStrToStrArrayFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBindVariableRejected() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setStr(0, "{a,b}");
            assertException("SELECT 'a' <> all($1::text[]) FROM long_sequence(1)", 18, "constant expected");
        });
    }

    @Test
    public void testColumnRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c CHAR, ip IPV4, s STRING, sym SYMBOL, u UUID, vc VARCHAR)");
            for (String column : new String[]{"c", "ip", "s", "sym", "u", "vc"}) {
                assertException("SELECT 'a' <> all(" + column + "::text[]) FROM t", 18, "constant expected");
            }
        });
    }

    @Test
    public void testTextConstant() throws Exception {
        assertMemoryLeak(() -> assertQuery("""
                SELECT 'a' <> all('{a,b}'::text[]) s, 'a' <> all('{b,c}'::varchar::text[]) vc, 'a' <> all('{a}'::symbol::text[]) sym
                FROM long_sequence(1)
                """)
                .noLeakCheck()
                .expectSize()
                .returns("""
                        s\tvc\tsym
                        false\ttrue\tfalse
                        """));
    }
}
