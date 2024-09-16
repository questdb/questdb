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
import io.questdb.griffin.engine.functions.finance.FormatPriceFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class FormatPriceFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testFormatPriceHappyPath() throws Exception {
        assertQuery("format_price\n100-16\n", "select format_price(100.5, 32)");
        assertQuery("format_price\n101-00\n", "select format_price(101.0, 32)");
        assertQuery("format_price\n101-00\n", "select format_price(101, 32)");
        assertQuery("format_price\n99-12\n", "select format_price(99.375, 32)");
        assertQuery("format_price\n102-09\n", "select format_price(102.28125, 32)");
    }

    @Test
    public void testNullBehavior() throws Exception {
        final String expected = "format_price\nNULL\n";
        assertQuery(expected, "select format_price(1.0, NULL)");
        assertQuery(expected, "select format_price(1.5, NULL)");
        assertQuery(expected, "select format_price(NULL, NULL)");

    }

    @Test
    public void testNegativeTickSize() throws Exception {
        final String expected = "format_price\nNULL\n";
        assertQuery(expected, "select format_price(1.0, -2)");
        assertQuery(expected, "select format_price(1.5, -1)");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new FormatPriceFunctionFactory();
    }
}
