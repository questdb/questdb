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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class FunctionParserErrorTest extends AbstractCairoTest {

    @Test
    public void testFunctionParserErrorIsNotPersistent() throws Exception {
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck(
                    "select * from " +
                            "(select cast(x as timestamp) ts, '0x05cb69971d94a00000192178ef80f0' as id, x from long_sequence(10) ) " +
                            "where ts between '2022-03-20' AND id <> '0x05ab6d9fabdabb00066a5db735d17a' AND id <> '0x05aba84839b9c7000006765675e630' AND id <> '0x05abc58d80ba1f000001ed05351873'",
                    153,
                    "there is no matching operator `!=` with the argument types: BOOLEAN != STRING"
            );
            runTestQuery();
        });
    }

    @Test
    public void testFunctionParserErrorIsNotPersistent2() throws Exception {
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck(
                    "select abs(ln(1,2), 4) + 10+'asdf' from long_sequence(1);",
                    11,
                    "wrong number of arguments for function `ln`; expected: 1, provided: 2"

            );
            runTestQuery();
        });
    }

    @Test
    public void testFunctionParserErrorIsNotPersistent3() throws Exception {
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck(
                    "select abs(1,2,3,4) from long_sequence(1)",
                    7,
                    "there is no matching function `abs` with the argument types: (INT, INT, INT, INT)"
            );
            runTestQuery();
        });
    }

    private void runTestQuery() throws Exception {
        assertQueryNoLeakCheck(
                "x\n1\n",
                "select x from long_sequence(1) where x < 10 and x > 0",
                null,
                null,
                true,
                false
        );
    }
}
