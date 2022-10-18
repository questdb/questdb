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

public class WidthBucketFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testFloatPositive() throws SqlException {
        assertQuery(
                "ceil\n" +
                        "14.0000\n",
                "select ceil(13.1f)",
                null,
                true,
                true
        );
    }

    @Test
    public void testBelowRange() throws SqlException {
        call(0.235, 0.893, 0.926, 4).andAssert(0);
    }

    @Test
    public void testLowerRange() throws SqlException {
        call(3.2456, 3.2456, 5.7344, 3).andAssert(1);
    }

    @Test
    public void testInRangeEdge() throws SqlException {
        call(86.432, 61.572, 111.292, 4).andAssert(3);
    }

    @Test
    public void testInRangeCenter() throws SqlException {
        call(88.599, 66.367, 109.551, 7).andAssert(4);
    }

    @Test
    public void testUpperRange() throws SqlException {
        call(39.068, 13.763, 39.068, 5).andAssert(6);
    }

    @Test
    public void testAboveRange() throws SqlException {
        call(108.233, 53.169, 91.209, 8).andAssert(9);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new WidthBucketFunctionFactory();
    }
}
