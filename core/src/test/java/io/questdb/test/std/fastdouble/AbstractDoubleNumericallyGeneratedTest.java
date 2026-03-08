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

package io.questdb.test.std.fastdouble;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.NumericException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

abstract class AbstractDoubleNumericallyGeneratedTest {

    private static final Log LOG = LogFactory.getLog(AbstractDoubleNumericallyGeneratedTest.class);

    /**
     * Seed for random number generator.
     * Specify a literal number to obtain repeatable tests.
     * Specify System.nanoTime to explore the input space.
     * (Make sure to take a note of the seed value if
     * tests failed.)
     */
    private static final long SEED = System.nanoTime();

    @BeforeClass
    public static void init() {
        LOG.info().$("seed=").$(SEED).$();
    }

    @Test
    public void testRandomDecimalFloatLiterals() {
        Random r = new Random(SEED);
        r.longs(10_000)
                .mapToDouble(Double::longBitsToDouble)
                .forEach(this::testLegalInput);
    }

    @Test
    public void testRandomHexadecimalFloatLiterals() {
        Random r = new Random(SEED);
        r.longs(10_000)
                .mapToDouble(Double::longBitsToDouble)
                .forEach(this::testLegalInput);
    }

    private void testLegalInput(String str, double expected) {
        double actual;
        try {
            actual = parse(str);
        } catch (NumericException e) {
            throw new NumberFormatException();
        }
        assertEquals("str=" + str, expected, actual, 0.001);
        assertEquals("longBits of " + expected, Double.doubleToLongBits(expected), Double.doubleToLongBits(actual));
    }

    private void testLegalInput(double expected) {
        testLegalInput(expected + "", expected);
    }

    protected abstract double parse(String str) throws NumericException;
}