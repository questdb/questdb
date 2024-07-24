/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
            ddl("create table x (a double, b double)");
            insert("insert into x values(null, null)");
            assertSql("a\tb\n", "select * from x where a <> b");
            assertSql("a\tb\nnull\tnull\n", "select * from x where a = b");
        });
    }

    @Test
    public void testFloatNullNotNullEquals() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (a float, b float)");
            insert("insert into x values(null, 1.0)");
            insert("insert into x values(3.14, 1.0)");
            assertSql("a\tb\n" +
                    "null\t1.0000\n" +
                    "3.1400\t1.0000\n", "select * from x where a <> b");
            assertSql("a\tb\n", "select * from x where a = b");
        });
    }

    @Test
    public void testFloatNullsEquals() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (a float, b float)");
            insert("insert into x values(null, null)");
            assertSql("a\tb\n", "select * from x where a <> b");
            assertSql("a\tb\nnull\tnull\n", "select * from x where a = b");
        });
    }
}
