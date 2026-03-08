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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.math.MulLongFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class MulLongFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testBothNan() throws SqlException {
        call(Numbers.LONG_NULL, Numbers.LONG_NULL).andAssert(Numbers.LONG_NULL);
    }

    @Test
    public void testLeftNan() throws SqlException {
        call(Numbers.LONG_NULL, 5L).andAssert(Numbers.LONG_NULL);
    }

    @Test
    public void testMulByZero() throws SqlException {
        call(10L, 0L).andAssert(0L);
    }

    @Test
    public void testRightNan() throws SqlException {
        call(123L, Numbers.LONG_NULL).andAssert(Numbers.LONG_NULL);
    }

    @Test
    public void testSimple() throws SqlException {
        call(10L, 81L).andAssert(810L);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new MulLongFunctionFactory();
    }
}