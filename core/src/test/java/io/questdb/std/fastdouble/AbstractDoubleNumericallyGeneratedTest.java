/*
 * @(#)NumericallyGeneratedTest.java
 * Copyright © 2021. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.NumericException;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

abstract class AbstractDoubleNumericallyGeneratedTest {
    /**
     * Seed for random number generator.
     * Specify a literal number to obtain repeatable tests.
     * Specify System.nanoTime to explore the input space.
     * (Make sure to take a note of the seed value if
     * tests failed.)
     */
    public static final long SEED = 0;//System.nanoTime();

    @TestFactory
    Stream<DynamicNode> dynamicTestsRandomDecimalFloatLiterals() {
        Random r = new Random(SEED);
        return r.longs(10_000)
                .mapToDouble(Double::longBitsToDouble)
                .mapToObj(d -> dynamicTest(d + "", () -> testLegalInput(d)));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsRandomHexadecimalFloatLiterals() {
        Random r = new Random(SEED);
        return r.longs(10_000)
                .mapToDouble(Double::longBitsToDouble)
                .mapToObj(d -> dynamicTest(Double.toHexString(d) + "", () -> testLegalInput(d)));
    }

    protected abstract double parse(String str) throws NumericException;

    private void testLegalInput(String str, double expected) throws NumericException {
        double actual = parse(str);
        assertEquals(expected, actual, "str=" + str);
        assertEquals(Double.doubleToLongBits(expected), Double.doubleToLongBits(actual),
                "longBits of " + expected);
    }

    private void testLegalInput(double expected) throws NumericException {
        testLegalInput(expected + "", expected);
    }
}