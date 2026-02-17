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

package io.questdb.test.griffin.engine.functions.finance;


import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.finance.SpreadFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class SpreadFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testNullBehavior() throws Exception {
        final String expected = "spread\nnull\n";
        assertQuery(expected, "select spread(NULL, 1.0)");
        assertQuery(expected, "select spread(1.0, NULL)");
        assertQuery(expected, "select spread(NULL, NULL)");
    }

    @Test
    public void testSpread() throws Exception {
        assertQuery("spread\n0.0\n", "select spread(2.0, 2.0)");
        assertQuery("spread\n2.0\n", "select spread(1.0, 3.0)");
        assertQuery("spread\n4.0\n", "select spread(0.0, 4.0)");
        assertQuery("spread\n1.0\n", "select spread(1.0, 2.0)");
        assertQuery("spread\n0.25\n", "select spread(1.5, 1.75)");
        assertQuery("spread\n0.1100000000000001\n", "select spread(1.5, 1.61)");
        assertQuery("spread\n0.0\n", "select spread(0.0,0.0)");
        assertQuery("spread\n2.0\n", "select spread(-1.0,1.0)");
        assertQuery("spread\n1.0\n", "select spread(-1.0,0.0)");
        assertQuery("spread\n1.0\n", "select spread(-2.0,-1.0)");
        assertQuery("spread\n1.1111090000000001\n", "select spread(-2.22222,-1.111111)");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new SpreadFunctionFactory();
    }
}
