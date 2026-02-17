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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class EqVarcharFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testVarcharEqualsToStr() throws Exception {
        String aaLines = "x\tk\n" +
                "1\taa\n" +
                "6\taa\n" +
                "9\taa\n" +
                "12\taa\n" +
                "17\taa\n";
        assertQuery(
                aaLines,
                "select x, k from x where k = 'aa'",
                "create table x as (select x, rnd_varchar('aa', 'abcабв你好\uD83D\uDE00', null, 'абв') k, rnd_str('aa', 'abcабв你好\uD83D\uDE00', null, 'абв') ks from long_sequence(20))",
                null,
                true,
                false
        );

        assertQuery(
                aaLines,
                "select x, k from x where 'aa' = k",
                null,
                true,
                false
        );

        assertQuery(
                "x\tks\n" +
                        "5\taa\n" +
                        "6\taa\n" +
                        "8\taa\n" +
                        "13\taa\n" +
                        "14\taa\n" +
                        "15\taa\n" +
                        "19\taa\n" +
                        "20\taa\n",
                "select x, ks from x where cast('aa' as varchar) = ks",
                null,
                true,
                false
        );

        assertQuery(
                "x\tk\n" +
                        "4\t\n" +
                        "5\t\n" +
                        "7\t\n" +
                        "14\t\n" +
                        "18\t\n",
                "select x, k from x where k is null",
                null,
                true,
                false
        );

        assertQuery(
                "x\tks\n" +
                        "1\t\n" +
                        "10\t\n",
                "select x, ks from x where ks = cast(null as varchar)",
                null,
                true,
                false
        );

        assertQuery(
                "x\tk\tks\n" +
                        "6\taa\taa\n" +
                        "11\tабв\tабв\n",
                "select x, k, ks from x where k = ks",
                null,
                true,
                false
        );

        assertQuery(
                "x\tk\tks\n" +
                        "6\taa\taa\n" +
                        "11\tабв\tабв\n",
                "select x, k, ks from x where ks = k",
                null,
                true,
                false
        );

        assertQuery(
                "x\tk\tks\n" +
                        "6\taa\taa\n" +
                        "11\tабв\tабв\n",
                "select x, k, ks from x where ks = k",
                null,
                true,
                false
        );
    }

}
