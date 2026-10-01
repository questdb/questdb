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

package io.questdb.test.griffin.engine.functions.finance;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.finance.MidPriceFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class MidPriceFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testMidPrice() throws Exception {
        assertQuery("select mid(2.0, 2.0)").expectSize().returns("mid\n2.0\n");
        assertQuery("select mid(1.0, 3.0)").expectSize().returns("mid\n2.0\n");
        assertQuery("select mid(0.0, 4.0)").expectSize().returns("mid\n2.0\n");
        assertQuery("select mid(1.0, 2.0)").expectSize().returns("mid\n1.5\n");
        assertQuery("select mid(1.5, 1.75)").expectSize().returns("mid\n1.625\n");
        assertQuery("select mid(1.5, 1.61)").expectSize().returns("mid\n1.5550000000000002\n");
        assertQuery("select mid(0.0,0.0)").expectSize().returns("mid\n0.0\n");
        assertQuery("select mid(-1.0,1.0)").expectSize().returns("mid\n0.0\n");
        assertQuery("select mid(-1.0,0.0)").expectSize().returns("mid\n-0.5\n");
        assertQuery("select mid(-2.0,-1.0)").expectSize().returns("mid\n-1.5\n");
        assertQuery("select mid(-2.22222,-1.111111)").expectSize().returns("mid\n-1.6666655000000001\n");
    }

    @Test
    public void testNonFiniteNumber() throws Exception {
        final String expected = "mid\nnull\n";
        assertQuery("select mid(NULL, 1.0)").expectSize().returns(expected);
        assertQuery("select mid(1.0, NULL)").expectSize().returns(expected);
        assertQuery("select mid(NULL, NULL)").expectSize().returns(expected);
    }

    @Test
    public void testNullBehavior() throws Exception {
        final String expected = "mid\nnull\n";
        assertQuery("select mid(NULL, 1.0)").expectSize().returns(expected);
        assertQuery("select mid(1.0, NULL)").expectSize().returns(expected);
        assertQuery("select mid(NULL, NULL)").expectSize().returns(expected);

    }

    @Test
    public void testThatOrderDoesNotMatter() throws Exception {
        final String expected = "mid\n2.0\n";
        assertQuery("select mid(1.0, 3.0)").expectSize().returns(expected);
        assertQuery("select mid(3.0, 1.0)").expectSize().returns(expected);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new MidPriceFunctionFactory();
    }
}
