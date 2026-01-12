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

package io.questdb.test.griffin.engine.functions.eq;


import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class EqSymFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testLargeSymbolTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol(4000,1,7,3) a, rnd_symbol(4000,1,7,3) b from long_sequence(5000))");
            assertQuery(
                    """
                            count
                            288
                            """,
                    "select count() from x where a = b",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testSmoke() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5',null) a, rnd_symbol('1','4','5',null) b from long_sequence(50))");
            assertQuery(
                    """
                            a\tb
                            1\t1
                            \t
                            1\t1
                            \t
                            5\t5
                            \t
                            5\t5
                            \t
                            1\t1
                            """,
                    "select * from x where a = b",
                    null,
                    true,
                    false
            );
        });
    }
}
