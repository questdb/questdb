/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class LastBooleanGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testKeyed() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "a\tlast\n" +
                        "0\tfalse\n" +
                        "3\ttrue\n" +
                        "1\ttrue\n" +
                        "2\ttrue\n",
                "select a, last(sym) from tab",
                "create table tab as (select rnd_int() % 5 a, rnd_boolean() sym from long_sequence(5))",
                null,
                true,
                true,
                true
        ));
        assertMemoryLeak(() -> assertQuery(
                "a\tlast\n",
                "select a, last(sym) from tab2",
                "create table tab2 as (select rnd_int() % 5 a, rnd_boolean() sym from long_sequence(0))",
                null,
                true,
                true,
                true
        ));
        assertMemoryLeak(() -> assertQuery(
                "last\nfalse\n",
                "select last(sym) from tab3",
                "create table tab3 as (select rnd_int() % 5 a, rnd_boolean() sym from long_sequence(0))",
                null,
                false,
                true,
                true
        ));
        assertMemoryLeak(() -> assertQuery(
                "last\nfalse\n",
                "select last(sym) from tab4",
                "create table tab4 as (select rnd_int() % 5 a, rnd_boolean() sym from long_sequence(10))",
                null,
                false,
                true,
                true
        ));
    }
}