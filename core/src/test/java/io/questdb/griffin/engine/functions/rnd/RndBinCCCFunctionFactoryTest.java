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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.griffin.engine.functions.math.NegIntFunctionFactory;
import org.junit.Test;

public class RndBinCCCFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testAllNulls() throws Exception {
        testNullRate("testCol\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n",
                1);
    }

    @Test
    public void testNoNulls() throws Exception {
        testNullRate("testCol\n" +
                        "00000000 ee 41 1d 15 55 8a 17\n" +
                        "00000000 d8 cc 14 ce f1\n" +
                        "00000000 88 c4 91 3b 72 db\n" +
                        "00000000 04 1b c7 88 de a0\n" +
                        "00000000 3c 77 15 68 61 26\n" +
                        "00000000 19 c4 95 94 36 53 49 b4\n" +
                        "00000000 7e 3b 08 a1 1e 38 8d 1b\n" +
                        "00000000 f4 c8 39 09 fe d8 9d\n" +
                        "00000000 78 36 6a\n" +
                        "00000000 de e4 7c\n",
                0);
    }

    @Test
    public void testWithNulls() throws Exception {
        testNullRate("testCol\n" +
                        "\n" +
                        "00000000 1d 15 55 8a 17 fa\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "00000000 59 88 c4 91 3b 72\n" +
                        "00000000 04 1b c7 88 de a0\n" +
                        "00000000 77 15 68\n" +
                        "00000000 af 19 c4 95 94 36 53\n" +
                        "00000000 59 7e 3b 08 a1\n",
                4);
    }


    @Test
    public void testAllNullsFixedLength() throws Exception {
        testNullRateFixedLength("testCol\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n",
                1);
    }

    @Test
    public void testNoNullsFixedLength() throws Exception {
        testNullRateFixedLength("testCol\n" +
                        "00000000 50 ee 41 1d 15\n" +
                        "00000000 55 8a 17 fa d8\n" +
                        "00000000 cc 14 ce f1 59\n" +
                        "00000000 88 c4 91 3b 72\n" +
                        "00000000 db f3 04 1b c7\n" +
                        "00000000 88 de a0 79 3c\n" +
                        "00000000 77 15 68 61 26\n" +
                        "00000000 af 19 c4 95 94\n" +
                        "00000000 36 53 49 b4 59\n" +
                        "00000000 7e 3b 08 a1 1e\n",
                0);
    }

    @Test
    public void testWithNullsFixedLength() throws Exception {
        testNullRateFixedLength("testCol\n" +
                        "\n" +
                        "00000000 41 1d 15 55 8a\n" +
                        "00000000 fa d8 cc 14 ce\n" +
                        "00000000 59 88 c4 91 3b\n" +
                        "00000000 db f3 04 1b c7\n" +
                        "\n" +
                        "00000000 a0 79 3c 77 15\n" +
                        "\n" +
                        "00000000 26 af 19 c4 95\n" +
                        "\n",
                4);
    }

    @Test
    public void testNegativeNullRate() {
        assertFailure("[19] invalid null rate", "select rnd_bin(3,6,-1) as testCol from long_sequence(20)");
    }

    @Test
    public void testBadMinimum() {
        assertFailure(8, "minimum has to be grater than 0", 0L, 10L, 2);
    }

    @Test
    public void testInvalidRange() {
        assertFailure(0, "invalid range", 150L, 140L, 3);
    }

    @Override
    protected void addExtraFunctions() {
        functions.add(new NegIntFunctionFactory());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndBinCCCFunctionFactory();
    }

    private void testNullRate(CharSequence expectedData, int nullRate) throws Exception {
        assertQuery(expectedData, "select to_char(rnd_bin(3,8," + nullRate + ")) as testCol from long_sequence(10)");
    }

    private void testNullRateFixedLength(CharSequence expectedData, int nullRate) throws Exception {
        assertQuery(expectedData, "select to_char(rnd_bin(5,5," + nullRate + ")) as testCol from long_sequence(10)");
    }
}
