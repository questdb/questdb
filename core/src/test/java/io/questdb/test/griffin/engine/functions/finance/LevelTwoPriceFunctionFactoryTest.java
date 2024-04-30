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
import io.questdb.griffin.engine.functions.finance.LevelTwoPriceFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class LevelTwoPriceFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testLevelTwoPrice() throws Exception {
        assertQuery("l2price\n" +
                "9.825714285714286\n", "select l2price(35, 8, 5.2, 23, 9.3, 42, 22.1)");
    }

    @Test
    public void testLevelTwoPriceWithAggregate() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as ( select timestamp_sequence(172800000000, 3600000000) ts, rnd_long(12, 20, 0) as size, rnd_double() as value from long_sequence(10))");
            drainWalQueue();

            assertQuery("avg\n"
                    + "0.4494059253582213\n", "select avg(l2price(14, size, value)) from x");
        });
    }

    @Test
    public void testLevelTwoPriceFailsWithEvenArgs() throws Exception {
        assertException("select l2price(35, 8);", 19, "l2price requires an odd number of arguments.");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new LevelTwoPriceFunctionFactory();
    }


}