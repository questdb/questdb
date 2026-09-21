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

public class NullEqualsTest extends AbstractCairoTest {

    @Test
    public void testDoubleNullsEquals() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (a double, b double)");
            execute("insert into x values(null, null)");
            assertQuery("select * from x where a <> b")
                    .noLeakCheck()
                    .returns("a\tb\n");
            assertQuery("select * from x where a = b")
                    .noLeakCheck()
                    .returns("a\tb\nnull\tnull\n");
        });
    }

    @Test
    public void testFloatNullNotNullEquals() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (a float, b float)");
            execute("insert into x values(null, 1.0)");
            execute("insert into x values(3.14, 1.0)");
            assertQuery("select * from x where a <> b")
                    .noLeakCheck()
                    .returns("""
                            a\tb
                            null\t1.0
                            3.14\t1.0
                            """);
            assertQuery("select * from x where a = b")
                    .noLeakCheck()
                    .returns("a\tb\n");
        });
    }

    @Test
    public void testFloatNullsEquals() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (a float, b float)");
            execute("insert into x values(null, null)");
            assertQuery("select * from x where a <> b")
                    .noLeakCheck()
                    .returns("a\tb\n");
            assertQuery("select * from x where a = b")
                    .noLeakCheck()
                    .returns("a\tb\nnull\tnull\n");
        });
    }
}
