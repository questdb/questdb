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

public class RndDoubleCCFunctionFactoryTest extends AbstractFunctionFactoryTest {

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
                        "0.6607777894187332\n" +
                        "0.2246301342497259\n" +
                        "0.08486964232560668\n" +
                        "0.299199045961845\n" +
                        "0.20447441837877756\n" +
                        "0.6508594025855301\n" +
                        "0.8423410920883345\n" +
                        "0.9856290845874263\n" +
                        "0.22452340856088226\n" +
                        "0.5093827001617407\n" +
                        "0.6254021542412018\n" +
                        "0.7611029514995744\n" +
                        "0.5243722859289777\n" +
                        "0.5599161804800813\n" +
                        "0.0367581207471136\n" +
                        "0.2390529010846525\n" +
                        "0.4224356661645131\n" +
                        "0.6778564558839208\n" +
                        "0.3100545983862456\n" +
                        "0.38539947865244994\n",
                0);
    }

    @Test
    public void testWithNaNs() throws Exception {
        testNaNRate("testCol\n" +
                        "NaN\n" +
                        "0.2246301342497259\n" +
                        "0.2845577791213847\n" +
                        "0.20447441837877756\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "0.7905675319675964\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "0.5093827001617407\n" +
                        "0.11427984775756228\n" +
                        "0.5243722859289777\n" +
                        "0.8072372233384567\n" +
                        "NaN\n" +
                        "0.6276954028373309\n" +
                        "0.6778564558839208\n" +
                        "0.8756771741121929\n" +
                        "NaN\n" +
                        "0.0035983672154330515\n" +
                        "NaN\n",
                4);
    }

    @Test
    public void testNegativeNaNRate() {
        assertFailure("[18] invalid NaN rate", "select rnd_double(-2) as testCol from long_sequence(20)");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndDoubleCCFunctionFactory();
    }

    private void testNaNRate(CharSequence expectedData, int nanRate) throws Exception {
        assertQuery(expectedData, "select rnd_double(" + nanRate + ") as testCol from long_sequence(20)");
    }
}
