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

public class RoundDoubleFunctionFactoryConstTest extends AbstractCairoTest {

    @Test
    public void testLargeNegScale() throws Exception {
        assertQuery(
                "round\n" +
                        "null\n",
                "select round(14.7778, -18) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testLargePosScale() throws Exception {
        assertQuery(
                "round\n" +
                        "null\n",
                "select round(14.7778, 17) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testNegScaleHigherThanNumber() throws Exception {
        assertQuery(
                "round\n" +
                        "-0.0\n",
                "select round(-14.778, -5) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testNegScaleNegValue() throws Exception {
        assertQuery(
                "round\n" +
                        "-100.0\n",
                "select round(-104.9, -1) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testNegScaleNegValue2() throws Exception {
        assertQuery(
                "round\n" +
                        "-110.0\n",
                "select round(-106.1, -1) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testNegScalePosValue() throws Exception {
        assertQuery(
                "round\n" +
                        "100.0\n",
                "select round(104.90, -1) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testNegScalePosValue2() throws Exception {
        assertQuery(
                "round\n" +
                        "110.0\n",
                "select round(106.1, -1) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testOKNegScale() throws Exception {
        assertQuery(
                "round\n" +
                        "0.0\n",
                "select round(14.7778, -13) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testOKPosScale() throws Exception {
        assertQuery(
                "round\n" +
                        "14.7778\n",
                "select round(14.7778, 11) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testPosScaleHigherThanNumber() throws Exception {
        assertQuery(
                "round\n" +
                        "-14.777999999999999\n",
                "select round(-14.778, 7) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testPosScaleNegValue() throws Exception {
        assertQuery(
                "round\n" +
                        "-100.5\n",
                "select round(-100.54, 1) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testPosScaleNegValue2() throws Exception {
        assertQuery(
                "round\n" +
                        "-100.60000000000001\n",
                "select round(-100.56, 1) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testPosScalePosValue() throws Exception {
        assertQuery(
                "round\n" +
                        "100.4\n",
                "select round(100.44, 1) from long_sequence(1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testPosScalePosValue2() throws Exception {
        assertQuery(
                "round\n" +
                        "100.5\n",
                "select round(100.45, 1) from long_sequence(1)",
                null,
                true,
                true
        );
    }
}
