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
import io.questdb.griffin.engine.functions.math.DivFloatFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class DivFloatFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testDivByZero() throws SqlException {
        call(10f, 0f).andAssert(Float.NaN, 0.000001);
    }

    @Test
    public void testNegative() throws SqlException {
        call(-3f, 4f).andAssert(-0.75, 0.000001);
    }

    @Test
    public void testSimple() throws SqlException {
        call(10f, 8f).andAssert(1.25, 0.000001);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new DivFloatFunctionFactory();
    }
}