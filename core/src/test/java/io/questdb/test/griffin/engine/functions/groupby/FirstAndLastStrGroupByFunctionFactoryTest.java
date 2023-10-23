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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class FirstAndLastStrGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNull() throws Exception {
        assertQuery("r1\tr2\n" +
                        "\t\n",
                "select first(a1)r1,last(a1)r2 from tab",
                "create table tab as (select cast(list(null,null,null) as string)a1 from long_sequence(3))",
                null,
                false,
                true
        );
    }

    @Test
    public void testFirstSomethingLastNull() throws Exception {
        assertQuery("r1\tr2\n" +
                        "something\t\n",
                "select first(a1)r1,last(a1)r2 from tab",
                "create table tab as (select cast(list('something','else',null) as string)a1 from long_sequence(3))",
                null,
                false,
                true
        );
    }

    @Test
    public void testFirstNullLastSomething() throws Exception {
        assertQuery("r1\tr2\n" +
                        "\tsomething\n",
                "select first(a1)r1,last(a1)r2 from tab",
                "create table tab as (select cast(list(null,'else','something') as string)a1 from long_sequence(3))",
                null,
                false,
                true
        );
    }
}
