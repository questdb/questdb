/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class CursorDereferenceFunctionFactoryTest extends AbstractGriffinTest {
    @Test
    public void testCatalogue() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pg_test(a int)", sqlExecutionContext);
            assertQuery(
                    "x\tpg_class\n" +
                            "11\t\n" +
                            "2200\t\n" +
                            "11\t\n" +
                            "2200\t\n" +
                            "11\t\n" +
                            "2200\t\n" +
                            "11\t\n" +
                            "2200\t\n" +
                            "11\t\n" +
                            "2200\t\n" +
                            "11\t\n" +
                            "2200\t\n" +
                            "11\t\n" +
                            "2200\t\n" +
                            "11\t\n" +
                            "2200\t\n" +
                            "11\t\n" +
                            "2200\t\n" +
                            "11\t\n" +
                            "2200\t\n",
                    "select (pg_catalog.pg_class()).relnamespace x,  pg_catalog.pg_class() from long_sequence(10);",
                    null,
                    false,
                    false
            );
        });
    }
}