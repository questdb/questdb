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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class RoundHalfEvenDoubleFunctionFactoryConstTest extends AbstractCairoTest {

    @Test
    public void testLargeNegScale() throws Exception {
        assertQuery(
                "round_half_even\n" +
                        "null\n",
                "select round_half_even(14.7778, -18) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testLargePosScale() throws Exception {
        assertQuery(
                "round_half_even\n" +
                        "null\n",
                "select round_half_even(14.7778, 18) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testNegScaleHigherThanNumber() throws Exception {
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
    public void testNegScaleNegValueRoundsDown() throws Exception {
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
    public void testNegScaleNegValueRoundsUp() throws Exception {
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
    public void testNegScalePosValueRoundsDown() throws Exception {
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
    public void testNegScalePosValueRoundsUp() throws Exception {
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
    public void testOKNegScale() throws Exception {
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
    public void testOKPosScale() throws Exception {
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
    public void testOKPosScale17() throws Exception {
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
    public void testPosScaleNegValueRoundsDown() throws Exception {
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
    public void testPosScaleNegValueRoundsUp() throws Exception {
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
    public void testPosScalePosValueRoundsDown() throws Exception {
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
    public void testPosScalePosValueRoundsUp() throws Exception {
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
    public void testZeroScalePosValueRoundsDown() throws Exception {
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
    public void testZeroScalePosValueRoundsUp() throws Exception {
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
    public void testZeroScaleScaleNegValueRoundsDown() throws Exception {
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
    public void testZeroScaleScaleNegValueRoundsUp() throws Exception {
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
