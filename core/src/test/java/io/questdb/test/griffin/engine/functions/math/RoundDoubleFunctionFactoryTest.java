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
import io.questdb.griffin.engine.functions.math.RoundDoubleFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class RoundDoubleFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testLargeNegScale() throws SqlException {
        call(14.7778, -18).andAssert(Double.NaN, 0.0000000001);
    }

    /*Bounds*/
    @Test
    public void testLargePosScale() throws SqlException {
        call(14.7778, 17).andAssert(Double.NaN, 0.0000000001);
    }

    @Test
    public void testLeftNan() throws SqlException {
        call(Double.NaN, 5).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testNegScaleHigherThanNumber() throws SqlException {
        call(14.7778, -5).andAssert(0, 0.0000000001);
    }

    @Test
    public void testNegScaleNegValue() throws SqlException {
        call(-104.9, -1).andAssert(-100.0, 0.0000000001);
    }

    @Test
    public void testNegScaleNegValue2() throws SqlException {
        call(-106.1, -1).andAssert(-110, 0.0000000001);
    }

    @Test
    public void testNegScalePosValue() throws SqlException {
        call(104.9, -1).andAssert(100, 0.0000000001);
    }

    @Test
    public void testNegScalePosValue2() throws SqlException {
        call(106.1, -1).andAssert(110, 0.0000000001);
    }

    @Test
    public void testOKNegScale() throws SqlException {
        call(14.7778, -13).andAssert(0.0, 0.0000000001);
    }

    @Test
    public void testOKPosScale() throws SqlException {
        call(14.7778, 11).andAssert(14.7778, 0.0000000001);
    }

    @Test
    public void testPosScaleHigherThanNumber() throws SqlException {
        call(14.7778, 7).andAssert(14.7778, 0.000001);
    }

    @Test
    public void testPosScaleNegValue() throws SqlException {
        call(-100.54, 1).andAssert(-100.5, 0.0000000001);
    }

    @Test
    public void testPosScaleNegValue2() throws SqlException {
        call(-100.56, 1).andAssert(-100.6, 0.0000000001);
    }

    @Test
    public void testPosScalePosValue() throws SqlException {
        call(100.44, 1).andAssert(100.4, 0.0000000001);
    }

    @Test
    public void testPosScalePosValue2() throws SqlException {
        call(100.45, 1).andAssert(100.5, 0.0000000001);
    }

    @Test
    public void testRightNan() throws SqlException {
        call(123.65, Numbers.INT_NULL).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testSimple() throws SqlException {
        call(14.7778, 3).andAssert(14.778, 0.0000000001);
    }

    @Test
    public void testSimpleZeroScale() throws SqlException {
        call(14.7778, 0).andAssert(15, 0.0000000001);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RoundDoubleFunctionFactory();
    }
}