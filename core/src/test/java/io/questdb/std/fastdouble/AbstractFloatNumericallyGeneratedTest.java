/*
 * @(#)NumericallyGeneratedTest.java
 * Copyright © 2021. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.NumericException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

abstract class AbstractFloatNumericallyGeneratedTest {

    private static final Log LOG = LogFactory.getLog(AbstractFloatNumericallyGeneratedTest.class);

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
        r.ints(10_000)
                .mapToObj(Float::intBitsToFloat)
                .forEach(this::testLegalInput);
    }

    @Test
    public void testRandomHexadecimalFloatLiterals() {
        Random r = new Random(SEED);
        r.ints(10_000)
                .mapToObj(Float::intBitsToFloat)
                .forEach(this::testLegalInput);
    }

    protected abstract float parse(String str) throws NumericException;

    private void testLegalInput(String str, float expected) {
        float actual;
        try {
            actual = parse(str);
        } catch (NumericException e) {
            throw new NumberFormatException();
        }
        assertEquals("str=" + str, expected, actual, 0.001);
        assertEquals("intBits of " + expected, Float.floatToIntBits(expected), Float.floatToIntBits(actual));
    }

    private void testLegalInput(float expected) {
        testLegalInput(expected + "", expected);
    }
}