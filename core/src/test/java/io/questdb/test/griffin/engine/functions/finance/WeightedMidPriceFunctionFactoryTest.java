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
        assertQuery("wmid\n2.0\n", "select wmid(2.0, 100, 2.0, 100)");
        assertQuery("wmid\n2.0\n", "select wmid(1.0, 300, 3.0, 200)");
        assertQuery("wmid\n2.0\n", "select wmid(0.0, 400, 4.0, 300)");
        assertQuery("wmid\n1.5\n", "select wmid(1.0, 100, 2.0, 100)");
        assertQuery("wmid\n1.625\n", "select wmid(1.5, 100, 1.75, 1000)");
        assertQuery("wmid\n1.5550000000000002\n", "select wmid(1.5, 100, 1.61, 100)");
        assertQuery("wmid\n0.0\n", "select wmid(0.0, 200, 0.0, 0)");
        assertQuery("wmid\n0.0\n", "select wmid(-1.0, 200, 1.0, 100)");
        assertQuery("wmid\n-0.5\n", "select wmid(-1.0, 100, 0.0, -0.5)");
        assertQuery("wmid\n-1.5\n", "select wmid(-2.0, 100, -1.0, 100)");
        assertQuery("wmid\n-1.6666655000000001\n", "select wmid(-2.22222, 100, -1.111111, 200)");
    }

    @Test
    public void testNonFiniteNumber() throws Exception {
        final String expected = "wmid\nnull\n";
        assertQuery(expected, "select wmid(NULL, 1.0)");
        assertQuery(expected, "select wmid(1.0, NULL)");
        assertQuery(expected, "select wmid(NULL, NULL)");
    }

    @Test
    public void testNullBehavior() throws Exception {
        final String expected = "wmid\nnull\n";
        assertQuery(expected, "select wmid(NULL, 100, 1.0, 100");
        assertQuery(expected, "select wmid(1.0, NULL, 1.0, 100");
        assertQuery(expected, "select wmid(1.0, 100, NULL, 100");
        assertQuery(expected, "select wmid(1.0, 100, 1.0, NULL");
        assertQuery(expected, "select wmid(NULL, NULL)");

    }

    @Test
    public void testThatOrderDoesNotMatter() throws Exception {
        final String expected = "wmid\n4.0\n";
        assertQuery(expected, "select wmid(1.0, 100, 3.0, 100)");
        assertQuery(expected, "select wmid(3.0, 100, 1.0, 100)");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new WeightedMidPriceFunctionFactory();
    }
}