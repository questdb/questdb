/*+*****************************************************************************
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
import io.questdb.griffin.engine.functions.math.WidthBucketFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class WidthBucketFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testAscendingRange() throws SqlException {
        call(5.3, 1.0, 10.0, 5).andAssert(3);
    }

    @Test
    public void testDescendingRange() throws SqlException {
        call(5.3, 10.0, 1.0, 5).andAssert(3);
    }

    @Test
    public void testAscendingUnderflow() throws SqlException {
        call(0.5, 1.0, 10.0, 5).andAssert(0);
    }

    @Test
    public void testDescendingUnderflow() throws SqlException {
        call(11.0, 10.0, 1.0, 5).andAssert(0);
    }

    @Test
    public void testAscendingOverflow() throws SqlException {
        call(10.0, 1.0, 10.0, 5).andAssert(6);
        call(15.0, 1.0, 10.0, 5).andAssert(6);
    }

    @Test
    public void testDescendingOverflow() throws SqlException {
        call(1.0, 10.0, 1.0, 5).andAssert(6);
        call(0.5, 10.0, 1.0, 5).andAssert(6);
    }

    @Test
    public void testFirstBucketBoundary() throws SqlException {
        call(1.0, 1.0, 10.0, 5).andAssert(1);
    }

    @Test
    public void testLastBucketBoundary() throws SqlException {
        call(9.999, 1.0, 10.0, 5).andAssert(5);
    }

    @Test
    public void testNaNOperandReturnsNull() throws SqlException {
        call(Double.NaN, 1.0, 10.0, 5)
                .andAssert(Numbers.INT_NULL);
    }

    @Test
    public void testNaNLowerBoundReturnsNull() throws SqlException {
        call(5.0, Double.NaN, 10.0, 5)
                .andAssert(Numbers.INT_NULL);
    }

    @Test
    public void testNaNUpperBoundReturnsNull() throws SqlException {
        call(5.0, 1.0, Double.NaN, 5)
                .andAssert(Numbers.INT_NULL);
    }

    @Test
    public void testNullCountReturnsNull() throws SqlException {
        call(5.0, 1.0, 10.0, Numbers.INT_NULL)
                .andAssert(Numbers.INT_NULL);
    }

    @Test
    public void testRuntimeNegativeCountReturnsNull() throws SqlException {
        call(5.0, 1.0, 10.0, -1)
                .andAssert(Numbers.INT_NULL);
    }



    @Test
    public void testConstantZeroCountFails() {
      assertFailure(
            true,
            26,
            "bucket count must be greater than 0",
            5.0, 1.0, 10.0, 0
        );
    }

    @Test
    public void testConstantEqualBoundsFail() {
      assertFailure(
            true,
            21,
            "bounds cannot be equal",
            5.0, 5.0, 5.0, 5
        );
    }
    @Override
    protected FunctionFactory getFunctionFactory() {
        return new WidthBucketFunctionFactory();
    }
}
