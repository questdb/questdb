/*
 * @(#)HandPickedTest.java
 * Copyright © 2021. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.NumericException;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

abstract class AbstractFloatHandPickedTest {

    /**
     * Tests input classes that execute different code branches in
     * method {@link FastFloatMath#tryDecToFloatWithFastAlgorithm(boolean, long, int)}.
     */
    @TestFactory
    List<DynamicNode> dynamicTestsDecFloatLiteralClingerInputClasses() {
        return Arrays.asList(
                //
                dynamicTest("Inside Clinger fast path (max_clinger_significand, max_clinger_exponent)", () -> testLegalInput(
                        "16777215e10")),
                dynamicTest("Outside Clinger fast path (max_clinger_significand, max_clinger_exponent + 1)", () -> testLegalInput(
                        "16777215e11")),
                dynamicTest("Outside Clinger fast path (max_clinger_significand + 1, max_clinger_exponent)", () -> testLegalInput(
                        "16777216e10")),
                dynamicTest("Inside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent)", () -> testLegalInput(
                        "1e-10")),
                dynamicTest("Outside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent - 1)", () -> testLegalInput(
                        "1e-11"))
        );
    }

    /**
     * Tests input classes that execute different code branches in
     * method {@link FastDoubleMath#tryHexFloatToDouble(boolean, long, int)}.
     */
    @TestFactory
    List<DynamicNode> dynamicTestsHexFloatLiteralClingerInputClasses() {
        return Arrays.asList(
                dynamicTest("Inside Clinger fast path (max_clinger_significand)", () -> testLegalInput(
                        "0x1fffffffffffffp74", 0x1fffffffffffffp74f)),
                dynamicTest("Outside Clinger fast path (max_clinger_significand, max_clinger_exponent + 1)", () -> testLegalInput(
                        "0x1fffffffffffffp74", 0x1fffffffffffffp74f)),
                dynamicTest("Outside Clinger fast path (max_clinger_significand + 1, max_clinger_exponent)", () -> testLegalInput(
                        "0x20000000000000p74", 0x20000000000000p74f)),
                dynamicTest("Inside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent)", () -> testLegalInput(
                        "0x1p-74", 0x1p-74f)),
                dynamicTest("Outside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent - 1)", () -> testLegalInput(
                        "0x1p-75", 0x1p-75f))
        );
    }

    @TestFactory
    List<DynamicNode> dynamicTestsIllegalInputs() {
        return Arrays.asList(
                dynamicTest("0.+(char)0x3231+(char)0x0000+345678",
                        () -> testIllegalInput("0." + (char) 0x3231 + (char) 0x0000 + "345678")),

                dynamicTest("empty", () -> testIllegalInput("")),
                dynamicTest("-", () -> testIllegalInput("-")),
                dynamicTest("+", () -> testIllegalInput("+")),
                dynamicTest("1e", () -> testIllegalInput("1e")),
                dynamicTest("1ee2", () -> testIllegalInput("1ee2")),
                dynamicTest("1_000", () -> testIllegalInput("1_000")),
                dynamicTest("0.000_1", () -> testIllegalInput("0.000_1")),
                dynamicTest("-e-55", () -> testIllegalInput("-e-55")),
                dynamicTest("1 x", () -> testIllegalInput("1 x")),
                dynamicTest("x 1", () -> testIllegalInput("x 1")),
                dynamicTest("1§", () -> testIllegalInput("1§")),
                dynamicTest("NaN x", () -> testIllegalInput("NaN x")),
                dynamicTest("Infinity x", () -> testIllegalInput("Infinity x")),
                dynamicTest("0x123.456789abcde", () -> testIllegalInput("0x123.456789abcde")),
                dynamicTest(".", () -> testIllegalInput(".")),
                dynamicTest("0x.", () -> testIllegalInput("0x.")),
                dynamicTest(".e2", () -> testIllegalInput(".e2"))
        );
    }

    @TestFactory
    List<DynamicNode> dynamicTestsIllegalInputsWithPrefixAndSuffix() {
        return Arrays.asList(
                dynamicTest("before-after", () -> testIllegalInputWithPrefixAndSuffix("before-after", 6, 1)),
                dynamicTest("before7.78$after", () -> testIllegalInputWithPrefixAndSuffix("before7.78$after", 6, 5)),
                dynamicTest("before7.78e$after", () -> testIllegalInputWithPrefixAndSuffix("before7.78e$after", 6, 6)),
                dynamicTest("before0x123$4after", () -> testIllegalInputWithPrefixAndSuffix("before0x123$4after", 6, 7)),
                dynamicTest("before0x123.4$after", () -> testIllegalInputWithPrefixAndSuffix("before0x123.4$after", 6, 8)),
                dynamicTest("before0$123.4after", () -> testIllegalInputWithPrefixAndSuffix("before0$123.4after", 6, 7))
        );
    }

    @TestFactory
    List<DynamicNode> dynamicTestsLegalDecFloatLiterals() {
        return Arrays.asList(
                dynamicTest("-0.0", () -> testLegalInput("-0.0", -0.0f)),
                dynamicTest("0.12345678", () -> testLegalInput("0.12345678", 0.12345678f)),
                dynamicTest("1e23", () -> testLegalInput("1e23", 1e23f)),
                dynamicTest("whitespace before 1", () -> testLegalInput(" 1")),
                dynamicTest("whitespace after 1", () -> testLegalInput("1 ")),
                dynamicTest("0", () -> testLegalInput("0", 0.0f)),
                dynamicTest("-0", () -> testLegalInput("-0", -0.0f)),
                dynamicTest("+0", () -> testLegalInput("+0", +0.0f)),
                dynamicTest("-0.0", () -> testLegalInput("-0.0", -0.0f)),
                dynamicTest("-0.0e-22", () -> testLegalInput("-0.0e-22", -0.0e-22f)),
                dynamicTest("-0.0e24", () -> testLegalInput("-0.0e24", -0.0e24f)),
                dynamicTest("0e555", () -> testLegalInput("0e555", 0.0f)),
                dynamicTest("-0e555", () -> testLegalInput("-0e555", -0.0f)),
                dynamicTest("1", () -> testLegalInput("1", 1.0f)),
                dynamicTest("-1", () -> testLegalInput("-1", -1.0f)),
                dynamicTest("+1", () -> testLegalInput("+1", +1.0f)),
                dynamicTest("1e0", () -> testLegalInput("1e0", 1e0f)),
                dynamicTest("1.e0", () -> testLegalInput("1.e0", 1e0f)),
                dynamicTest("1e1", () -> testLegalInput("1e1", 1e1f)),
                dynamicTest("1e+1", () -> testLegalInput("1e+1", 1e+1f)),
                dynamicTest("1e-1", () -> testLegalInput("1e-1", 1e-1f)),
                dynamicTest("0049", () -> testLegalInput("0049", 49f)),
                dynamicTest("9999999999999999999", () -> testLegalInput("9999999999999999999", 9999999999999999999f)),
                dynamicTest("972150611626518208.0", () -> testLegalInput("972150611626518208.0", 9.7215061162651827E17f)),
                dynamicTest("0.1e+3", () -> testLegalInput("0.1e+3",
                        100.0f)),
                dynamicTest("0.00000000000000000000000000000000000000000001e+46",
                        () -> testLegalInput("0.00000000000000000000000000000000000000000001e+46",
                                100.0f)),
                dynamicTest("10000000000000000000000000000000000000000000e+308",
                        () -> testLegalInput("10000000000000000000000000000000000000000000e+308",
                                Float.parseFloat("10000000000000000000000000000000000000000000e+308"))),
                dynamicTest("3.1415926535897932384626433832795028841971693993751", () -> testLegalInput(
                        "3.1415926535897932384626433832795028841971693993751",
                        Float.parseFloat("3.1415926535897932384626433832795028841971693993751"))),
                dynamicTest("314159265358979323846.26433832795028841971693993751e-20", () -> testLegalInput(
                        "314159265358979323846.26433832795028841971693993751e-20",
                        3.141592653589793f)),
                dynamicTest("1e-326", () -> testLegalInput(
                        "1e-326", 0.0f)),
                dynamicTest("1e-325f", () -> testLegalInput(
                        "1e-325", 0.0f)),
                dynamicTest("1e310", () -> testLegalInput(
                        "1e310", Float.POSITIVE_INFINITY)),
                dynamicTest(7.2057594037927933e+16 + "", () -> testLegalDecInput(
                        7.2057594037927933e+16f)),
                dynamicTest(-7.2057594037927933e+16 + "", () -> testLegalDecInput(
                        -7.2057594037927933e+16f))
        );
    }

    @TestFactory
    List<DynamicNode> dynamicTestsLegalDecFloatLiteralsExtremeValues() {
        return Arrays.asList(
                dynamicTest(Float.toString(Float.MIN_VALUE), () -> testLegalDecInput(
                        Float.MIN_VALUE)),
                dynamicTest(Float.toString(Float.MAX_VALUE), () -> testLegalDecInput(
                        Float.MAX_VALUE)),
                dynamicTest(Float.toString(Float.POSITIVE_INFINITY), () -> testLegalDecInput(
                        Float.POSITIVE_INFINITY)),
                dynamicTest(Float.toString(Float.NEGATIVE_INFINITY), () -> testLegalDecInput(
                        Float.NEGATIVE_INFINITY)),
                dynamicTest(Float.toString(Float.NaN), () -> testLegalDecInput(
                        Float.NaN)),
                dynamicTest(Float.toString(Math.nextUp(0.0f)), () -> testLegalDecInput(
                        Math.nextUp(0.0f))),
                dynamicTest(Float.toString(Math.nextDown(0.0f)), () -> testLegalDecInput(
                        Math.nextDown(0.0f))),
                dynamicTest("Just above MAX_VALUE: 3.4028236e+38f", () -> testLegalInput(
                        "3.4028236e+38", Float.POSITIVE_INFINITY)),
                dynamicTest("Just below MIN_VALUE:  1.3e-45f", () -> testLegalInput(
                        "0.7e-45", 0.0f))
        );
    }

    @TestFactory
    List<DynamicNode> dynamicTestsLegalHexFloatLiterals() {
        return Arrays.asList(
                dynamicTest("0x0.1234ab78p0", () -> testLegalInput("0x0.1234ab78p0", 0x0.1234ab78p0f)),
                dynamicTest("0x0.1234AB78p0", () -> testLegalInput("0x0.1234AB78p0", 0x0.1234AB78p0f)),
                dynamicTest("0x1.0p8", () -> testLegalInput("0x1.0p8", 256f))
        );
    }

    @TestFactory
    List<DynamicNode> dynamicTestsLegalHexFloatLiteralsExtremeValues() {
        return Arrays.asList(
                dynamicTest(Float.toHexString(Float.MIN_VALUE), () -> testLegalHexInput(
                        Float.MIN_VALUE)),
                dynamicTest(Float.toHexString(Float.MAX_VALUE), () -> testLegalHexInput(
                        Float.MAX_VALUE)),
                dynamicTest(Float.toHexString(Float.POSITIVE_INFINITY), () -> testLegalHexInput(
                        Float.POSITIVE_INFINITY)),
                dynamicTest(Float.toHexString(Float.NEGATIVE_INFINITY), () -> testLegalHexInput(
                        Float.NEGATIVE_INFINITY)),
                dynamicTest(Float.toHexString(Float.NaN), () -> testLegalHexInput(
                        Float.NaN)),
                dynamicTest(Float.toHexString(Math.nextUp(0.0f)), () -> testLegalHexInput(
                        Math.nextUp(0.0f))),
                dynamicTest(Float.toHexString(Math.nextDown(0.0f)), () -> testLegalHexInput(
                        Math.nextDown(0.0f))),
                dynamicTest("Just above MAX_VALUE: 0x1.fffffffffffff8p1023", () -> testLegalInput(
                        "0x1.fffffffffffff8p1023", Float.POSITIVE_INFINITY)),
                dynamicTest("Just below MIN_VALUE: 0x0.00000000000008p-1022", () -> testLegalInput(
                        "0x0.00000000000008p-1022", 0.0f))

        );
    }

    @TestFactory
    List<DynamicNode> dynamicTestsLegalInputsWithPrefixAndSuffix() {
        return Arrays.asList(
                dynamicTest("before-1after", () -> testLegalInputWithPrefixAndSuffix("before-1after", 6, 2, -1.0f)),
                dynamicTest("before7.789after", () -> testLegalInputWithPrefixAndSuffix("before7.789after", 6, 5, 7.789f)),
                dynamicTest("before7.78e2after", () -> testLegalInputWithPrefixAndSuffix("before7.78e2after", 6, 6, 7.78e2f)),
                dynamicTest("before0x123.4p0after", () -> testLegalInputWithPrefixAndSuffix("before0x1234p0after", 6, 8, 0x1234p0f)),
                dynamicTest("before0x123.45p0after", () -> testLegalInputWithPrefixAndSuffix("before0x123.45p0after", 6, 10, 0x123.45p0f)),
                dynamicTest("Outside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent - 1)", () -> testLegalInputWithPrefixAndSuffix(
                        "before1e-23after", 6, 5, 1e-23f))

        );
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsPowerOfTen() {
        return IntStream.range(-307, 309).mapToObj(i -> "1e" + i)
                .map(d -> dynamicTest(d, () -> testLegalInput(d, Float.parseFloat(d))));
    }

    abstract float parse(CharSequence str) throws NumericException;

    protected abstract float parse(String str, int offset, int length) throws NumericException;

    private void testIllegalInput(String s) {
        try {
            parse(s);
            fail();
        } catch (NumericException e) {
            // success
        }
    }

    private void testIllegalInputWithPrefixAndSuffix(String str, int offset, int length) {
        assertThrows(NumericException.class, () -> parse(str, offset, length));
    }

    private void testLegalDecInput(float expected) throws NumericException {
        testLegalInput(expected + "", expected);
    }

    private void testLegalHexInput(float expected) throws NumericException {
        testLegalInput(Float.toHexString(expected), expected);
    }

    private void testLegalInput(String str) throws NumericException {
        testLegalInput(str, Float.parseFloat(str));
    }

    private void testLegalInput(String str, float expected) throws NumericException {
        float actual = parse(str);
        assertEquals(expected, actual, "str=" + str);
        assertEquals(Float.floatToIntBits(expected), Float.floatToIntBits(actual),
                "intBits of " + expected);
    }

    private void testLegalInputWithPrefixAndSuffix(String str, int offset, int length, float expected) throws NumericException {
        float actual = parse(str, offset, length);
        assertEquals(expected, actual);
    }

}