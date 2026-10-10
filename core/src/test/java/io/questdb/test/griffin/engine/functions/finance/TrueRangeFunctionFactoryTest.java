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
import io.questdb.griffin.engine.functions.finance.TrueRangeFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class TrueRangeFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testNullBehavior() throws Exception {
        final String expected = "true_range\nnull\n";
        assertQuery("select true_range(NULL, 1.0, 1.0)").expectSize().returns(expected);
        assertQuery("select true_range(1.0, NULL, 1.0)").expectSize().returns(expected);
        assertQuery("select true_range(1.0, 1.0, NULL)").expectSize().returns(expected);
        assertQuery("select true_range(NULL, NULL, NULL)").expectSize().returns(expected);
    }

    @Test
    public void testTrueRange() throws Exception {
        assertQuery("select true_range(10.0, 5.0, 8.0)").expectSize().returns("true_range\n5.0\n");
        assertQuery("select true_range(10.0, 8.0, 5.0)").expectSize().returns("true_range\n5.0\n");
        assertQuery("select true_range(8.0, 5.0, 10.0)").expectSize().returns("true_range\n5.0\n");
        assertQuery("select true_range(10.5, 9.5, 10.0)").expectSize().returns("true_range\n1.0\n");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new TrueRangeFunctionFactory();
    }
}
