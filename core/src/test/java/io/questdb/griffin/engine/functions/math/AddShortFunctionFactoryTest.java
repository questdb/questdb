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

package io.questdb.griffin.engine.functions.math;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.std.Numbers;
import org.junit.Test;

public class AddShortFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testLeftNan() throws SqlException {
        call(Numbers.SHORT_NaN, 5).andAssert(Numbers.SHORT_NaN);
    }

    @Test
    public void testNegative() throws SqlException {
        call(3, -4).andAssert(-1);
    }

    @Test
    public void testRightNan() throws SqlException {
        call(123, Numbers.SHORT_NaN).andAssert(Numbers.SHORT_NaN);
    }

    @Test
    public void testSimple() throws SqlException {
        call(45, 51).andAssert((short) 96);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new AddShortFunctionFactory();
    }
}