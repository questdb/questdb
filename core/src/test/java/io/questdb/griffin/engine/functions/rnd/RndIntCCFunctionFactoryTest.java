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
import org.junit.Test;

public class RndIntCCFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testAllNaNs() throws Exception {
        testNaNRate("testCol\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n",
                1);
    }

    @Test
    public void testNoNaNs() throws Exception {
        testNaNRate("testCol\n" +
                        "10\n" +
                        "58\n" +
                        "23\n" +
                        "81\n" +
                        "81\n" +
                        "39\n" +
                        "72\n" +
                        "71\n" +
                        "76\n" +
                        "68\n" +
                        "26\n" +
                        "42\n" +
                        "44\n" +
                        "83\n" +
                        "41\n" +
                        "52\n" +
                        "16\n" +
                        "47\n" +
                        "85\n" +
                        "14\n",
                0);
    }

    @Test
    public void testWithNaNs() throws Exception {
        testNaNRate("testCol\n" +
                        "NaN\n" +
                        "23\n" +
                        "81\n" +
                        "72\n" +
                        "76\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "83\n" +
                        "52\n" +
                        "NaN\n" +
                        "85\n" +
                        "17\n" +
                        "64\n" +
                        "67\n" +
                        "NaN\n" +
                        "68\n" +
                        "94\n" +
                        "71\n" +
                        "NaN\n",
                4);
    }

    @Test
    public void testNegativeNaNRate() {
        assertFailure("[21] invalid NaN rate", "select rnd_int(10,33,-1) as testCol from long_sequence(20)");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndIntCCFunctionFactory();
    }

    private void testNaNRate(CharSequence expectedData, int nanRate) throws Exception {
        assertQuery(expectedData, "select rnd_int(10,99," + nanRate + ") as testCol from long_sequence(20)");
    }
}
