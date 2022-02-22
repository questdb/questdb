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

public class RndFloatCFunctionFactoryTest extends AbstractFunctionFactoryTest {

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
                        "0.6608\n" +
                        "0.8043\n" +
                        "0.2246\n" +
                        "0.1297\n" +
                        "0.0849\n" +
                        "0.2846\n" +
                        "0.2992\n" +
                        "0.0844\n" +
                        "0.2045\n" +
                        "0.9345\n" +
                        "0.6509\n" +
                        "0.1312\n" +
                        "0.8423\n" +
                        "0.7906\n" +
                        "0.9856\n" +
                        "0.1920\n" +
                        "0.2245\n" +
                        "0.8899\n" +
                        "0.5094\n" +
                        "0.3491\n",
                0);
    }

    @Test
    public void testWithNaNs() throws Exception {
        testNaNRate("testCol\n" +
                        "NaN\n" +
                        "0.2246\n" +
                        "0.0849\n" +
                        "0.2992\n" +
                        "0.2045\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "0.7906\n" +
                        "0.1920\n" +
                        "NaN\n" +
                        "0.5094\n" +
                        "0.6254\n" +
                        "0.7611\n" +
                        "0.5244\n" +
                        "NaN\n" +
                        "0.8072\n" +
                        "0.7261\n" +
                        "0.6277\n" +
                        "NaN\n",
                4);
    }

    @Test
    public void testNegativeNaNRate() {
        assertFailure("[17] invalid NaN rate", "select rnd_float(-3) as testCol from long_sequence(20)");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndFloatCFunctionFactory();
    }

    private void testNaNRate(CharSequence expectedData, int nanRate) throws Exception {
        assertQuery(expectedData, "select rnd_float(" + nanRate + ") as testCol from long_sequence(20)");
    }
}
