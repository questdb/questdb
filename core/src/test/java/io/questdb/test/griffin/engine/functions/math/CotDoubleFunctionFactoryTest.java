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
import io.questdb.griffin.engine.functions.math.CotDoubleFunctionFactory;
import org.junit.Test;

public class CotDoubleFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testNaN() throws SqlException {
        call(Double.NaN).andAssert(Double.NaN, DELTA);
    }

    @Test
    public void testNegative() throws SqlException {
        call(-Math.PI).andAssert(8.165619676597685E15, DELTA);
        call(-Math.PI / 2).andAssert(-0.0, DELTA);
        call(-Math.PI * 3 / 4).andAssert(1.0, DELTA);

    }

    @Test
    public void testPositive() throws SqlException {
        call(Math.PI).andAssert(-8.165619676597685E15, DELTA);
        call(Math.PI / 2).andAssert(-0.0, DELTA);
        call(Math.PI * 3 / 4).andAssert(-1.0, DELTA);
    }

    @Test
    public void testZero() throws SqlException {
        call(0.0).andAssert(Double.POSITIVE_INFINITY, DELTA);
        call(-0.0000000000000001).andAssert(-1.0E16, DELTA);
        call(0.0000000000000001).andAssert(1.0E16, DELTA);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new CotDoubleFunctionFactory();
    }
}
