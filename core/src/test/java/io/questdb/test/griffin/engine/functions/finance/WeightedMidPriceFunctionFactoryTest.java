/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.griffin.engine.functions.finance.WeightedMidPriceFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class WeightedMidPriceFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testWeightedMidPrice() throws Exception {
        assertQuery("wmid\n2.0\n", "select wmid(100.0, 2.0, 2.0, 100.0)");
        assertQuery("wmid\n2.0\n", "select wmid(300.0, 1.0, 3.0, 200.0)");
        assertQuery("wmid\n2.0\n", "select wmid(400.0, 0.0, 4.0, 300.0)");
        assertQuery("wmid\n1.5\n", "select wmid(100.0, 1.0, 2.0, 100.0)");
        assertQuery("wmid\n1.625\n", "select wmid(100.0, 1.5, 1.75, 1000.0)");
        assertQuery("wmid\n1.5550000000000002\n", "select wmid(100.0, 1.5, 1.61, 100.5)");
        assertQuery("wmid\n0.0\n", "select wmid(200.0, 0.0, 0.0, 0.01)");
        assertQuery("wmid\n0.0\n", "select wmid(200.0, -1.0, 1.0, 100.0)");
        assertQuery("wmid\n-0.5\n", "select wmid(100.3, -1.0, 0.0, 200.1)");
        assertQuery("wmid\n-1.5\n", "select wmid(100.5, -2.0, -1.0, 100.4)");
        assertQuery("wmid\n-1.6666655000000001\n", "select wmid(100.2, -2.22222 -1.111111, 200.4)");
    }

    @Test
    public void testNonFiniteNumber() throws Exception {
        final String expected = "wmid\nnull\n";
        assertQuery(expected, "select wmid(NULL, 1.0, 1.0, 1.0)");
        assertQuery(expected, "select wmid(100.0, NULL, 1.0, 0.4)");
        assertQuery(expected, "select wmid(NULL, NULL, 1.0, 100.1)");
        assertQuery(expected, "select wmid(NULL, 1.0, NULL, 100.0)");
        assertQuery(expected, "select wmid(NULL, 1.0, 1.0, NULL)");
    }

    @Test
    public void testNullBehavior() throws Exception {
        final String expected = "wmid\nnull\n";
        assertQuery(expected, "select wmid(100.1, NULL, 1.0, 100.34");
        assertQuery(expected, "select wmid(NULL, 1.0, 1.0, 1.2");
        assertQuery(expected, "select wmid(100.3, 1.0, NULL, 100.0");
        assertQuery(expected, "select wmid(30.0, 1.0, 1.0, NULL");
        assertQuery(expected, "select wmid(NULL, NULL, NULL, NULL)");
    }

    @Test
    public void testThatOrderDoesNotMatter() throws Exception {
        final String expected = "wmid\n4.0\n";
        assertQuery(expected, "select wmid(100, 1.0, 3.0, 100.0)");
        assertQuery(expected, "select wmid(100, 3.0, 1.0, 100.0)");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new WeightedMidPriceFunctionFactory();
    }
}