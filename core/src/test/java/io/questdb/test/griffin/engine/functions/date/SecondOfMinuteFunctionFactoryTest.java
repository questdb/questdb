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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.test.AbstractGriffinTest;
import org.junit.Test;

public class SecondOfMinuteFunctionFactoryTest extends AbstractGriffinTest {
    @Test
    public void testNull() throws Exception {
        assertQuery(
                "second\n" +
                        "NaN\n",
                "select second(null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testPreEpoch() throws Exception {
        assertQuery(
                "second\n" +
                        "21\n",
                "select second('1901-07-11T22:00:21.555998Z'::timestamp)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanilla() throws Exception {
        assertQuery(
                "second\n" +
                        "30\n",
                "select second('1997-04-11T22:00:30.555555Z'::timestamp)",
                null,
                null,
                true,
                true
        );
    }
}
