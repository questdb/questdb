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

    protected abstract double parse(String str) throws NumericException;

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
}