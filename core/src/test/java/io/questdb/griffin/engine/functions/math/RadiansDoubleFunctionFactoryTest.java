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
import org.junit.Test;

public class RadiansDoubleFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testNaN() throws SqlException {
        call(Double.NaN).andAssert(Double.NaN, 0.000000000000001);
    }

    @Test
    public void testNegative() throws SqlException {
        call(-45).andAssert(-Math.PI / 4, 0.000000000000001);
        call(-90).andAssert(-Math.PI / 2, 0.000000000000001);
        call(-180).andAssert(-Math.PI, 0.000000000000001);
        call(-360).andAssert(-2 * Math.PI, 0.000000000000001);
    }

    @Test
    public void testPositive() throws SqlException {
        call(45).andAssert(Math.PI / 4, 0.000000000000001);
        call(90).andAssert(Math.PI / 2, 0.000000000000001);
        call(180).andAssert(Math.PI, 0.000000000000001);
        call(360).andAssert(2* Math.PI, 0.000000000000001);
    }

    @Test
    public void testZero() throws SqlException {
        call(0).andAssert(0.0, 0.000000000000001);
        call(0.0).andAssert(0.0, 0.000000000000001);
        call(-0).andAssert(0.0, 0.000000000000001);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RadiansDoubleFunctionFactory();
    }
}
