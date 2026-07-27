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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class EqVarcharFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testVarcharEqualsToStr() throws Exception {
        String aaLines = """
                x\tk
                1\taa
                6\taa
                9\taa
                12\taa
                17\taa
                """;
        assertQuery("select x, k from x where k = 'aa'")
                .ddl("create table x as (select x, rnd_varchar('aa', 'abcабв你好\uD83D\uDE00', null, 'абв') k, rnd_str('aa', 'abcабв你好\uD83D\uDE00', null, 'абв') ks from long_sequence(20))")
                .returns(aaLines);

        assertQuery("select x, k from x where 'aa' = k")
                .returns(aaLines);

        assertQuery("select x, ks from x where cast('aa' as varchar) = ks")
                .returns("""
                        x\tks
                        5\taa
                        6\taa
                        8\taa
                        13\taa
                        14\taa
                        15\taa
                        19\taa
                        20\taa
                        """);

        assertQuery("select x, k from x where k is null")
                .returns("""
                        x\tk
                        4\t
                        5\t
                        7\t
                        14\t
                        18\t
                        """);

        assertQuery("select x, ks from x where ks = cast(null as varchar)")
                .returns("""
                        x\tks
                        1\t
                        10\t
                        """);

        assertQuery("select x, k, ks from x where k = ks")
                .returns("""
                        x\tk\tks
                        6\taa\taa
                        11\tабв\tабв
                        """);

        assertQuery("select x, k, ks from x where ks = k")
                .returns("""
                        x\tk\tks
                        6\taa\taa
                        11\tабв\tабв
                        """);

        assertQuery("select x, k, ks from x where ks = k")
                .returns("""
                        x\tk\tks
                        6\taa\taa
                        11\tабв\tабв
                        """);
    }

}
