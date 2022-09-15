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

abstract class AbstractFloatNumericallyGeneratedTest {
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
        return r.ints(10_000)
                .mapToObj(Float::intBitsToFloat)
                .map(d -> dynamicTest(d + "", () -> testLegalInput(d)));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsRandomHexadecimalFloatLiterals() {
        Random r = new Random(SEED);
        return r.ints(10_000)
                .mapToObj(Float::intBitsToFloat)
                .map(d -> dynamicTest(Float.toHexString(d) + "", () -> testLegalInput(d)));
    }

    protected abstract float parse(String str) throws NumericException;

    private void testLegalInput(String str, float expected) throws NumericException {
        float actual = parse(str);
        assertEquals(expected, actual, "str=" + str);
        assertEquals(Float.floatToIntBits(expected), Float.floatToIntBits(actual),
                "intBits of " + expected);
    }

    private void testLegalInput(float expected) throws NumericException {
        testLegalInput(expected + "", expected);
    }
}