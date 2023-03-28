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
package io.questdb.test.griffin;

import io.questdb.test.AbstractGriffinTest;
import org.junit.Test;

public class SymbolTest extends AbstractGriffinTest {

    @Test
    public void testSelectSymbolUsingBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table logs ( id symbol capacity 2)");
            compile("insert into logs select x::string from long_sequence(10)");

            for (int i = 1; i < 11; i++) {
                assertQuery("id\n" + i + "\n", "select * from logs where id = '" + i + "'", null, true);
            }
        });
    }

    @Test
    public void testSelectSymbolUsingLiteral() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table logs ( id symbol capacity 2)");
            compile("insert into logs select x::string from long_sequence(10)");

            for (int i = 1; i < 11; i++) {
                bindVariableService.clear();
                bindVariableService.setStr("id", String.valueOf(i));

                assertQuery("id\n" + i + "\n", "select * from logs where id = :id", null, true);
            }
        });
    }
}
