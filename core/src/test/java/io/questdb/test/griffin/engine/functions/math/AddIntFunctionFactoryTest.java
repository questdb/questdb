/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.griffin.engine.functions.math.AddIntFunctionFactory;
import io.questdb.std.Numbers;
import org.junit.Test;

public class AddIntFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testLeftNull() throws SqlException {
        call(Numbers.INT_NaN, 10).andAssert(Numbers.INT_NaN);
    }

    @Test
    public void testOverflow() throws SqlException {
        call(5, Integer.MAX_VALUE).andAssert(-2147483644);
    }

    @Test
    public void testRightNull() throws SqlException {
        call(4, Numbers.INT_NaN).andAssert(Numbers.INT_NaN);
    }

    @Test
    public void testSimple() throws SqlException {
        call(5, 8).andAssert(13);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new AddIntFunctionFactory();
    }
}