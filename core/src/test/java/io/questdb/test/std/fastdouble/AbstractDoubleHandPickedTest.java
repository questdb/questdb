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

import io.questdb.std.NumericException;
import io.questdb.std.fastdouble.FastDoubleMath;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.Assert.*;

abstract class AbstractDoubleHandPickedTest {
    ///-----------old tests----------

    /**
     * Tests input classes that execute different code branches in
     * method {@link FastDoubleMath#tryDecFloatToDouble(boolean, long, int)}.
     * <p>
     * This test must achieve 100 % line coverage of the tested method.
     */
    @Test
    public void testDecFloatLiteralClingerInputClasses() throws NumericException {
        testLegalInput("Inside Clinger fast path \"1000000000000000000e-325\")", "1000000000000000000e-325");
        testLegalInput("Inside Clinger fast path (max_clinger_significand, max_clinger_exponent)", "9007199254740991e22");
        testLegalInput("Outside Clinger fast path (max_clinger_significand, max_clinger_exponent + 1)", "9007199254740991e23");
        testLegalInput("Outside Clinger fast path (max_clinger_significand + 1, max_clinger_exponent)", "9007199254740992e22");
        testLegalInput("Inside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent)", "1e-22");
        testLegalInput("Outside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent - 1)", "1e-23");
        testLegalInput("Outside Clinger fast path, semi-fast path, 9999999999999999999", "1e23");
        testLegalInput("Outside Clinger fast path, bail-out in semi-fast path, 1e23", "1e23");
        testLegalInput("Outside Clinger fast path, mantissa overflows in semi-fast path, 7.2057594037927933e+16", "7.2057594037927933e+16");
        testLegalInput("Outside Clinger fast path, bail-out in semi-fast path, 7.3177701707893310e+15", "7.3177701707893310e+15");
    }

    @Test
    public void testDecFloatLiteralNumericOverflow() throws NumericException {
        testOverflowInput("4.9E-325");
        testOverflowInput("-4.9E-325");
        testOverflowInput("1000000000000000000e-401");
        testOverflowInput("1.8E309");
        testOverflowInput("-1.8E309");
        testLegalInput("0.00000000d", "0.00000000d");
        testLegalInput("-0.00000000d", "-0.00000000d");
        testLegalInput("Infinity", "Infinity");
        testLegalInput("-Infinity", "-Infinity");
    }

    @Test
    public void testErrorCases() throws IOException {
        String file = TestUtils.getTestResourcePath("/fastdouble/FastDoubleParser_testcases.txt");
        Path p = Paths.get(file);
        Files.lines(p)
                .flatMap(line -> Arrays.stream(line.split(",")))
                .forEach(str -> {
                    try {
                        testLegalInput(str, Double.parseDouble(str));
                    } catch (NumericException e) {
                        throw new NumberFormatException();
                    }
                });
    }

    /**
     * Tests input classes that execute different code branches in
     * method {@link FastDoubleMath#tryHexFloatToDouble(boolean, long, int)}.
     * <p>
     * This test must achieve 100 % line coverage of the tested method.
     */
    @Test
    public void testHexFloatLiteralClingerInputClasses() throws NumericException {
        testLegalInput("Inside Clinger fast path (max_clinger_significand)", "0x1fffffffffffffp74", 0x1fffffffffffffp74);
        testLegalInput("Inside Clinger fast path (max_clinger_significand), negative", "-0x1fffffffffffffp74", -0x1fffffffffffffp74);
        testLegalInput("Outside Clinger fast path (max_clinger_significand, max_clinger_exponent + 1)", "0x1fffffffffffffp74", 0x1fffffffffffffp74);
        testLegalInput("Outside Clinger fast path (max_clinger_significand + 1, max_clinger_exponent)", "0x20000000000000p74", 0x20000000000000p74);
        testLegalInput("Inside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent)", "0x1p-74", 0x1p-74);
        testLegalInput("Outside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent - 1)", "0x1p-75", 0x1p-75);
    }

    @Test
    public void testHexFloatLiteralNumericOverflow() throws NumericException {
        System.out.println(Double.toHexString(Double.POSITIVE_INFINITY));
        testOverflowInput("0x0.0000000000001p-1023");
        testOverflowInput("-0x0.0000000000001p-1023");
        testOverflowInput("0x0.0000000000001p-1200");
        testLegalInput("0.00000000d", "0x00000000p0");
        testLegalInput("-0.00000000d", "-0x00000000p0");
    }

    @Test
    public void testIllegalInputs() {
        testIllegalInput("0." + (char) 0x3231 + (char) 0x0000 + "345678");
        testIllegalInput("");
        testIllegalInput("-");
        testIllegalInput("+");
        testIllegalInput("1e");
        testIllegalInput("1ee2");
        testIllegalInput("1_000");
        testIllegalInput("0.000_1");
        testIllegalInput("-e-55");
        testIllegalInput("1 x");
        testIllegalInput("x 1");
        testIllegalInput("1§");
        testIllegalInput("NaN x");
        testIllegalInput("Infinity x");
        testIllegalInput("0x123.456789abcde");
        testIllegalInput(".");
        testIllegalInput("0x.");
        testIllegalInput("0x");
        testIllegalInput("0x1");
        testIllegalInput("0xp1");
        testIllegalInput("0x1.");
        testIllegalInput(".e2");
    }

    @Test
    public void testIllegalInputsWithPrefixAndSuffix() {
        testIllegalInputWithPrefixAndSuffix("before-after", 1);
        testIllegalInputWithPrefixAndSuffix("before7.78$after", 5);
        testIllegalInputWithPrefixAndSuffix("before7.78e$after", 6);
        testIllegalInputWithPrefixAndSuffix("before0x123$4after", 7);
        testIllegalInputWithPrefixAndSuffix("before0x123.4$after", 8);
        testIllegalInputWithPrefixAndSuffix("before0$123.4after", 7);
    }

    @SuppressWarnings("UnpredictableBigDecimalConstructorCall")
    @Test
    public void testLegalBigDecimalValuesWithUnlimitedPrecision() throws NumericException {
        testLegalInput("-65.625", -65.625);
        testLegalInput(new BigDecimal(0.1).toString(), 0.1);
        testLegalInput(new BigDecimal(-Double.MIN_VALUE).toString(), -Double.MIN_VALUE);
        testLegalInput(new BigDecimal(-Double.MAX_VALUE).toString(), -Double.MAX_VALUE);
    }

    @Test
    public void testLegalDecFloatLiterals() throws NumericException {
        testLegalInput("-0.0", -0.0);
        testLegalInput("0.12345678", 0.12345678);
        testLegalInput("1e23", 1e23);
        testLegalInput("whitespace before 1", " 1");
        testLegalInput("whitespace after 1", "1 ");
        testLegalInput("0", 0.0);
        testLegalInput("-0", -0.0);
        testLegalInput("+0", +0.0);
        testLegalInput("-0.0", -0.0);
        testLegalInput("-0.0e-22", -0.0e-22);
        testLegalInput("-0.0e24", -0.0e24);
        testLegalInput("0e555", 0.0);
        testLegalInput("-0e555", -0.0);
        testLegalInput("1", 1.0);
        testLegalInput("-1", -1.0);
        testLegalInput("+1", +1.0);
        testLegalInput("1e0", 1e0);
        testLegalInput("1.e0", 1e0);
        testLegalInput(".8", 0.8);
        testLegalInput("8.", 8.0);
        testLegalInput("1e1", 1e1);
        testLegalInput("1e+1", 1e+1);
        testLegalInput("1e-1", 1e-1);
        testLegalInput("0049", 49);
        testLegalInput("9999999999999999999", 9999999999999999999d);
        testLegalInput("972150611626518208.0", 9.7215061162651827E17);
        testLegalInput("3.7587182468424695418288325e-309", 3.7587182468424695418288325e-309);
        testLegalInput("9007199254740992.e-256", 9007199254740992.e-256);
        testLegalInput("0.1e+3", 100.0);
        testLegalInput("0.00000000000000000000000000000000000000000001e+46", 100.0);
        testLegalInput("10000000000000000000000000000000000000000000e+308", Double.parseDouble("10000000000000000000000000000000000000000000e+308"));
        testLegalInput("3.1415926535897932384626433832795028841971693993751", Double.parseDouble("3.1415926535897932384626433832795028841971693993751"));
        testLegalInput("314159265358979323846.26433832795028841971693993751e-20", 3.141592653589793);
        testLegalInput("1e-326", 0.0);
        testLegalInput("1e-325", 0.0);
        testLegalInput("1e310", Double.POSITIVE_INFINITY);
        testLegalDecInput(7.2057594037927933e+16);
        testLegalDecInput(-7.2057594037927933e+16);
        testLegalDecInput(-4.8894481170331026E-173);
        testLegalDecInput(4.8894481170331026E-173);
        testLegalDecInput(-4.889448117033103E-173);
        testLegalDecInput(4.889448117033103E-173);
        testLegalDecInput(2.348957380189919E-199);
        testLegalDecInput(-2.348957380189919E-199);
        testLegalDecInput(-6.658066127037204E87);
        testLegalDecInput(6.658066127037204E87);
        testLegalDecInput(4.559067278662733E288);
        testLegalDecInput(-4.559067278662733E288);
    }

    @Test
    public void testLegalDecFloatLiteralsExtremeValues() throws NumericException {
        testLegalDecInput(Double.MIN_VALUE);
        testLegalDecInput(Double.MAX_VALUE);
        testLegalDecInput(Double.POSITIVE_INFINITY);
        testLegalDecInput(Double.NEGATIVE_INFINITY);
        testLegalDecInput(Double.NaN);
        testLegalDecInput(Math.nextUp(0.0));
        testLegalDecInput(Math.nextDown(0.0));
        testLegalInput("Just above MAX_VALUE: 1.7976931348623159E308", "1.7976931348623159E308", Double.POSITIVE_INFINITY);
        testLegalInput("Just below MIN_VALUE: 2.47E-324", "2.47E-324", 0.0);
    }

    @Test
    public void testLegalHexFloatLiterals() throws NumericException {
        testLegalInput("0x0.1234ab78p0", 0x0.1234ab78p0);
        testLegalInput("0x0.1234AB78p0", 0x0.1234AB78p0);
        testLegalInput("0x1.0p8", 256);
        testLegalInput("0x1.234567890abcdefp123", 0x1.234567890abcdefp123);
        testLegalInput("0x1234567890.abcdefp-45", 0x1234567890.abcdefp-45);
        testLegalInput("0x1234567890.abcdef12p-45", 0x1234567890.abcdef12p-45);
    }

    @Test
    public void testLegalHexFloatLiteralsExtremeValues() throws NumericException {
        testLegalHexInput(Double.MIN_VALUE);
        testLegalHexInput(Double.MAX_VALUE);
        testLegalHexInput(Double.POSITIVE_INFINITY);
        testLegalHexInput(Double.NEGATIVE_INFINITY);
        testLegalHexInput(Double.NaN);
        testLegalHexInput(Math.nextUp(0.0));
        testLegalHexInput(Math.nextDown(0.0));
        testLegalHexInput(Math.nextUp(0.0));
        testLegalHexInput(Math.nextDown(0.0));
        testLegalInput("Just above MAX_VALUE: 0x1.fffffffffffff8p1023", "0x1.fffffffffffff8p1023", Double.POSITIVE_INFINITY);
        testLegalInput("Just below MIN_VALUE: 0x0.00000000000008p-1022", "0x0.00000000000008p-1022", 0.0);
        testLegalInput("0X.2P102481", Double.POSITIVE_INFINITY);
    }

    @Test
    public void testLegalInputsWithPrefixAndSuffix() throws NumericException {
        testLegalInputWithPrefixAndSuffix("before-1after", 2, -1.0);
        testLegalInputWithPrefixAndSuffix("before7.789after", 5, 7.789);
        testLegalInputWithPrefixAndSuffix("before7.78e2after", 6, 7.78e2);
        testLegalInputWithPrefixAndSuffix("before0x1234p0after", 8, 0x1234p0);
        testLegalInputWithPrefixAndSuffix("before0x123.45p0after", 10, 0x123.45p0);
        testLegalInputWithPrefixAndSuffix("Outside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent - 1)",
                "before1e-23after", 5, 1e-23
        );
        testLegalInputWithPrefixAndSuffix("before9007199254740992.e-256after", 22, 9007199254740992.e-256);
    }

    @Test
    public void testPowerOfTen() throws NumericException {
        for (int i = -307; i < 309; i++) {
            final String d = "1e" + i;
            testLegalInput(d, Double.parseDouble(d));
        }
    }

    private void testIllegalInput(String s) {
        try {
            parse(s, false);
            fail();
        } catch (NumericException e) {
            // success
        }
    }

    private void testIllegalInputWithPrefixAndSuffix(String str, int length) {
        assertThrows(NumericException.class, () -> parse(str, 6, length, false));
    }

    private void testLegalDecInput(double expected) throws NumericException {
        final String s = expected + "";
        testLegalInput(s, s, expected);
    }

    private void testLegalHexInput(double expected) throws NumericException {
        testLegalInput(Double.toHexString(expected), Double.toHexString(expected), expected);
    }

    private void testLegalInput(String testName, String str) throws NumericException {
        testLegalInput(testName, str, Double.parseDouble(str));
    }

    private void testLegalInput(String str, double expected) throws NumericException {
        testLegalInput(str, str, expected);
    }

    private void testLegalInput(String testName, String str, double expected) throws NumericException {
        double actual = parse(str, false);
        assertEquals(testName, expected, actual, 0.001);
        assertEquals("longBits of " + expected, Double.doubleToLongBits(expected), Double.doubleToLongBits(actual));
    }

    private void testLegalInputWithPrefixAndSuffix(String str, int length, double expected) throws NumericException {
        testLegalInputWithPrefixAndSuffix(str, str, length, expected);
    }

    private void testLegalInputWithPrefixAndSuffix(String testName, String str, int length, double expected) throws NumericException {
        double actual = parse(str, 6, length, false);
        assertEquals(testName, expected, actual, 0.001);
    }

    private void testOverflowInput(String str) {
        try {
            parse(str, true);
            Assert.fail();
        } catch (NumericException ignored) {
        }
    }

    abstract double parse(CharSequence str, boolean rejectOverflow) throws NumericException;

    protected abstract double parse(String str, int offset, int length, boolean rejectOverflow) throws NumericException;
}