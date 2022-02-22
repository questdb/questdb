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

public class RndLongCCFunctionFactoryTest extends AbstractFunctionFactoryTest {

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
                        "266\n" +
                        "976\n" +
                        "745\n" +
                        "285\n" +
                        "189\n" +
                        "283\n" +
                        "258\n" +
                        "781\n" +
                        "96\n" +
                        "1076\n" +
                        "312\n" +
                        "1096\n" +
                        "604\n" +
                        "487\n" +
                        "19\n" +
                        "162\n" +
                        "570\n" +
                        "871\n" +
                        "121\n" +
                        "378\n",
                0);
    }

    @Test
    public void testWithNaNs() throws Exception {
        testNaNRate("testCol\n" +
                        "NaN\n" +
                        "745\n" +
                        "189\n" +
                        "258\n" +
                        "96\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "487\n" +
                        "162\n" +
                        "NaN\n" +
                        "121\n" +
                        "979\n" +
                        "426\n" +
                        "537\n" +
                        "NaN\n" +
                        "494\n" +
                        "442\n" +
                        "665\n" +
                        "NaN\n",
                4);
    }

    @Test
    public void testNegativeNaNRate() {
        assertFailure("[24] invalid NaN rate", "select rnd_long(10,1299,-1) as testCol from long_sequence(20)");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndLongCCFunctionFactory();
    }

    private void testNaNRate(CharSequence expectedData, int nanRate) throws Exception {
        assertQuery(expectedData, "select rnd_long(10,1299," + nanRate + ") as testCol from long_sequence(20)");
    }
}
