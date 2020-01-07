/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

public class RoundHalfEvenDoubleFunctionFactoryTest extends AbstractFunctionFactoryTest {

    /*Bounds*/
    @Test
    public void testLargeScale() throws SqlException {
        call(14.7778, 16).andAssert(Double.NaN, 0.0000000001);
    }

    @Test
    public void testLargeNegScale() throws SqlException {
        call(14.7778, -16).andAssert(Double.NaN, 0.0000000001);
    }

    /*Sanity*/
    @Test
    public void testLeftNan() throws SqlException {
        call(Double.NaN, 5).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testNegScaleNegValueRoundsDown() throws SqlException {
        call(-145, -1).andAssert(-140, 0.0000000001);
    }

    @Test
    public void testNegScaleHigherThanNumber() throws SqlException {
        call(14.7778, -5).andAssert(0, 0.0000000001);
    }

    /*Negative Scale*/

    @Test
    public void testPosScaleNegValueRoundDown() throws SqlException {
        call(-23.45, 1).andAssert(-23.4, 0.0000000001);
    }

    @Test
    public void testNegScaleNegValueRoundsUp() throws SqlException {
        call(-135, -1).andAssert(-140, 0.0000000001);
    }

    @Test
    public void testNegScalePosValueRoundsDown() throws SqlException {
        call(145, -1).andAssert(140, 0.0000000001);
    }

    @Test
    public void testNegScalePosValueRoundsUp() throws SqlException {
        call(135, -1).andAssert(140, 0.0000000001);
    }


    /*Positive Scale*/
    @Test
    public void testPosScaleNegValueRoundUp() throws SqlException {
        call(-23.35, 1).andAssert(-23.4, 0.0000000001);
    }

    @Test
    public void testPosScalePosValueRoundDown() throws SqlException {
        call(23.45, 1).andAssert(23.4, 0.0000000001);
    }

    @Test
    public void testPosScalePosValueRoundUp() throws SqlException {
        call(23.35, 1).andAssert(23.4, 0.0000000001);
    }

    @Test
    public void testRightNan() throws SqlException {
        call(123.65, Numbers.INT_NaN).andAssert(Double.NaN, 0.0001);
    }

    /*ZeroScale*/
    @Test
    public void testZeroScalePosValueRoundsDown() throws SqlException {
        call(24.5, 0).andAssert(24, 0.0000000001);
    }

    @Test
    public void testZeroScalePosValueRoundsUp() throws SqlException {
        call(23.5, 0).andAssert(24, 0.0000000001);
    }

    @Test
    public void testZeroScaleScaleNegValueRoundsDown() throws SqlException {
        call(-24.5, 0).andAssert(-24, 0.0000000001);
    }

    @Test
    public void testZeroScaleScaleNegValueRoundsUp() throws SqlException {
        call(-23.5, 0).andAssert(-24, 0.0000000001);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RoundHalfEvenDoubleFunctionFactory();
    }
}