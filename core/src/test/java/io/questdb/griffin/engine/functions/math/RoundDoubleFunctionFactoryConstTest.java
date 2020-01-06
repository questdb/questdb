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

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import org.junit.Test;

public class RoundDoubleFunctionFactoryConstTest extends AbstractGriffinTest {


    @Test
    public void testLargeNegScale() throws SqlException {
        assertQuery(
                "round\n" +
                        "NaN\n",
                "select round(14.778, -16) from long_sequence(1)",
                null,
                true
        );
    }

    @Test
    public void testLargePosScale() throws SqlException {
        assertQuery(
                "round\n" +
                        "NaN\n",
                "select round(14.778, 16) from long_sequence(1)",
                null,
                true
        );
    }

    @Test
    public void testNegScaleHigherThanNumber() throws SqlException {
        assertQuery(
                "round\n" +
                        "0.000000000000\n",
                "select round(-14.778, -5) from long_sequence(1)",
                null,
                true
        );
    }

    @Test
    public void testNegScaleNegValue() throws SqlException {
        assertQuery(
                "round\n" +
                        "-100.000000000000\n",
                "select round(-104.9, -1) from long_sequence(1)",
                null,
                true
        );
    }

    @Test
    public void testNegScaleNegValue2() throws SqlException {
        assertQuery(
                "round\n" +
                        "-110.000000000000\n",
                "select round(-106.1, -1) from long_sequence(1)",
                null,
                true
        );
    }

    @Test
    public void testNegScalePosValue() throws SqlException {
        assertQuery(
                "round\n" +
                        "100.000000000000\n",
                "select round(104.90, -1) from long_sequence(1)",
                null,
                true
        );
    }

    @Test
    public void testNegScalePosValue2() throws SqlException {
        assertQuery(
                "round\n" +
                        "110.000000000000\n",
                "select round(106.1, -1) from long_sequence(1)",
                null,
                true
        );
    }

    @Test
    public void testPosScaleHigherThanNumber() throws SqlException {
        assertQuery(
                "round\n" +
                        "-14.778000000000\n",
                "select round(-14.778, 7) from long_sequence(1)",
                null,
                true
        );
    }

    @Test
    public void testPosScaleNegValue() throws SqlException {
        assertQuery(
                "round\n" +
                        "-100.500000000000\n",
                "select round(-100.54, 1) from long_sequence(1)",
                null,
                true
        );
    }

    @Test
    public void testPosScaleNegValue2() throws SqlException {
        assertQuery(
                "round\n" +
                        "-100.600000000000\n",
                "select round(-100.56, 1) from long_sequence(1)",
                null,
                true
        );
    }

    @Test
    public void testPosScalePosValue() throws SqlException {
        assertQuery(
                "round\n" +
                        "100.400000000000\n",
                "select round(100.44, 1) from long_sequence(1)",
                null,
                true
        );
    }

    @Test
    public void testPosScalePosValue2() throws SqlException {
        assertQuery(
                "round\n" +
                        "100.500000000000\n",
                "select round(100.45, 1) from long_sequence(1)",
                null,
                true
        );
    }

}