/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.functions.array;


import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class DoubleArrayRoundFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testLargeNegScale() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[NaN]:DOUBLE[]\n",
                "select round(array[14.7778], -18)");
    }

    /*Bounds*/
    @Test
    public void testLargePosScale() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[NaN]:DOUBLE[]\n",
                "select round(array[14.7778], 17)");
    }

    @Test
    public void testLeftNan() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[NaN]:DOUBLE[]\n",
                "select round(array[NaN], 5)");
    }

    @Test
    public void testNegScaleHigherThanNumber() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[0.0]:DOUBLE[]\n",
                "select round(array[14.7778], -5)");
    }

    @Test
    public void testNegScaleNegValue() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[-100.0]:DOUBLE[]\n",
                "select round(array[-104.9], -1)");
    }

    @Test
    public void testNegScaleNegValue2() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[-110.0]:DOUBLE[]\n",
                "select round(array[-106.1], -1)");
    }

    @Test
    public void testNegScalePosValue() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[100.0]:DOUBLE[]\n",
                "select round(array[104.9], -1)");
    }

    @Test
    public void testNegScalePosValue2() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[110.0]:DOUBLE[]\n",
                "select round(array[106.1], -1)");
    }

    @Test
    public void testOKNegScale() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[0.0]:DOUBLE[]\n",
                "select round(array[14.7778], -13)");
    }

    @Test
    public void testOKPosScale() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[14.7778]:DOUBLE[]\n",
                "select round(array[14.7778], 11)");
    }

    @Test
    public void testPosScaleHigherThanNumber() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[14.7778]:DOUBLE[]\n",
                "select round(array[14.7778], 7)");
    }

    @Test
    public void testPosScaleNegValue() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[-100.5]:DOUBLE[]\n",
                "select round(array[-100.54], 1)");
    }

    @Test
    public void testPosScaleNegValue2() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[-100.60000000000001]:DOUBLE[]\n",
                "select round(array[-100.56], 1)");
    }

    @Test
    public void testPosScalePosValue() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[100.4]:DOUBLE[]\n",
                "select round(array[100.44], 1)");
    }

    @Test
    public void testPosScalePosValue2() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[100.5]:DOUBLE[]\n",
                "select round(array[100.45], 1)");
    }

    @Test
    public void testRightNan() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[NaN]:DOUBLE[]\n",
                "select round(array[123.65], null)");
    }

    @Test
    public void testSimple() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[14.778]:DOUBLE[]\n",
                "select round(array[14.7778], 3)");
    }

    @Test
    public void testSimpleZeroScale() throws SqlException {
        assertSqlWithTypes("round\n" +
                        "[15.0]:DOUBLE[]\n",
                "select round(array[14.7778],0)");
    }
}