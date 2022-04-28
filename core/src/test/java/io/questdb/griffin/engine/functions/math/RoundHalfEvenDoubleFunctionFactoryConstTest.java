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

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import org.junit.Test;

public class RoundHalfEvenDoubleFunctionFactoryConstTest extends AbstractGriffinTest {


    @Test
    public void testLargeNegScale() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "NaN\n",
                "select round_half_even(14.7778, -18) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testLargePosScale() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "NaN\n",
                "select round_half_even(14.7778, 18) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testNegScaleHigherThanNumber() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "0.0\n",
                "select round_half_even(14.778, -5) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testNegScaleNegValueRoundsDown() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "-140.0\n",
                "select round_half_even(-135, -1) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testNegScaleNegValueRoundsUp() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "-140.0\n",
                "select round_half_even(-145, -1) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testNegScalePosValueRoundsDown() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "140.0\n",
                "select round_half_even(135, -1) from long_sequence(1)",
                null,
                true,
                true
        );
    }


    /*Negative Scale*/

    @Test
    public void testNegScalePosValueRoundsUp() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "140.0\n",
                "select round_half_even(145, -1) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testOKNegScale() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "0.0\n",
                "select round_half_even(14.7778, -13) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testOKPosScale() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "14.7778\n",
                "select round_half_even(14.7778, 11) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testOKPosScale17() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "14.777800000000001\n",
                "select round_half_even(14.7778, 15) from long_sequence(1)",
                null,
                true,
                true
        );
    }


    /*Positive Scale*/

    @Test
    public void testPosScaleNegValueRoundsDown() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "-23.400000000000002\n",
                "select round_half_even(-23.35, 1) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testPosScaleNegValueRoundsUp() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "-23.400000000000002\n",
                "select round_half_even(-23.45, 1) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testPosScalePosValueRoundsDown() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "23.400000000000002\n",
                "select round_half_even(23.35, 1) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testPosScalePosValueRoundsUp() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "23.400000000000002\n",
                "select round_half_even(23.45, 1) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    /*Zero Scale*/

    @Test
    public void testZeroScalePosValueRoundsDown() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "24.0" +
                        "\n",
                "select round_half_even(24.5, 0) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testZeroScalePosValueRoundsUp() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "24.0\n",
                "select round_half_even(23.5, 0) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testZeroScaleScaleNegValueRoundsDown() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "-24.0\n",
                "select round_half_even(-24.5, 0) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testZeroScaleScaleNegValueRoundsUp() throws SqlException {
        assertQuery(
                "round_half_even\n" +
                        "-24.0\n",
                "select round_half_even(-23.5, 0) from long_sequence(1)",
                null,
                true,
                true
        );
    }


}