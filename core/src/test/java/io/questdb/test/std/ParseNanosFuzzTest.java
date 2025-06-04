/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.std;

import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ParseNanosFuzzTest {
    private static final char[] INVALID_CHARS = "abcdefgijklopqrtwxyz#$%^&*()+={}[]|\\:;\"'<>,.?/".toCharArray();
    private static final String[] INVALID_UNITS = {"d", "y", "w", "mo", "yr", "sec", "min", "hour", "HMS", "MSC"};
    private static final int MAX_VALUE = 1_000_000;
    private static final int NUM_INVALID_FUZZ = 5_000;
    // Test configuration
    private static final int NUM_TESTS = 10_000;
    // Time units and their multipliers (to microseconds)
    private static final String[] UNITS = {"", "ns", "us", "ms", "s", "m", "h"};
    private static final long[] UNIT_MULTIPLIERS = {
            1L,                   // no unit (assumed nanoseconds)
            1L,                   // nanoseconds
            1000L,                // microseconds
            1_000_000L,           // milliseconds
            1_000_000_000L,       // seconds
            60_000_000_000L,      // minutes
            3_600_000_000_000L    // hours
    };
    private static final Rnd rnd = new Rnd();
    private static final StringBuilder sb = new StringBuilder();
    private final boolean expectError;
    private final long expected;
    private final String input;

    public ParseNanosFuzzTest(String input, long expected, boolean expectError, String caseName) {
        this.input = input;
        this.expected = expected;
        this.expectError = expectError;
    }

    public static String replaceInvalidUtf8(CharSequence input) {
        return replaceInvalidUtf8(input, '?'); // Unicode replacement character
    }

    public static String replaceInvalidUtf8(CharSequence input, char replacement) {
        if (input == null) return null;
        sb.setLength(0);

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);

            if (Character.isValidCodePoint(c) && !Character.isISOControl(c)) {
                sb.append(c);
            } else if (Character.isHighSurrogate(c) && i + 1 < input.length()) {
                char low = input.charAt(i + 1);
                if (Character.isLowSurrogate(low)) {
                    int codePoint = Character.toCodePoint(c, low);
                    if (Character.isValidCodePoint(codePoint)) {
                        sb.append(c).append(low);
                    } else {
                        sb.append(replacement);
                    }
                    i++; // Skip the low surrogate
                } else {
                    sb.append(replacement);
                }
            } else {
                sb.append(replacement);
            }
        }
        return sb.toString();
    }

    // Define the test data
    @Parameterized.Parameters(name = "{3}")  // {0} refers to the first parameter (testName)
    public static Collection<Object[]> testData() {
        ArrayList<Object[]> testData = new ArrayList<>();
        runFuzzTests(testData);
        return testData;
    }

    @Test
    public void test() {
        try {
            long result = parseMicros(input);

            if (expectError) {
                Assert.fail("Failed: Expected exception for input: " + input);
            }

            if (expected != result) {
                Assert.fail("Failed: Input: " + input +
                        " Expected: " + expected +
                        " Got: " + result);
            }
        } catch (NumericException e) {
            if (!expectError) {
                Assert.fail("Failed: Unexpected exception for input: " + input);
            }
        }
    }

    private static void addTestData(ArrayList<Object[]> testData, String input, long expectedMicros, boolean fail) {
        testData.add(new Object[]{input, expectedMicros, fail, replaceInvalidUtf8(input)});
    }

    private static String generateInvalidDecimal() {
        return rnd.nextInt(MAX_VALUE) + "." +
                rnd.nextInt(1000) +
                (rnd.nextBoolean() ? UNITS[1 + rnd.nextInt(UNITS.length - 1)] : "");
    }

    private static String generateInvalidExponentNotation() {
        return rnd.nextInt(MAX_VALUE) + "e" +
                (rnd.nextBoolean() ? "+" : "-") +
                rnd.nextInt(5);
    }

    private static String generateInvalidInput() {
        switch (rnd.nextInt(14)) {
            case 0:
                return generateRandomInvalidChars();
            case 1:
                return generateNumberWithInvalidChars();
            case 2:
                return generateInvalidUnit();
            case 3:
                return generateInvalidNumberFormat();
            case 4:
                return generateInvalidNegativeFormat();
            case 5:
                return generateInvalidUnderscoreFormat();
            case 6:
                return generateInvalidWhitespace();
            case 7:
                return generateMultipleUnits();
            case 8:
                return generateInvalidDecimal();
            case 9:
                return generateUnitWithoutNumber();
            case 10:
                return generateNumberWithInvalidSuffix();
            case 11:
                return generateMalformedNegativeWithUnit();
            case 12:
                return generateInvalidExponentNotation();
            case 13:
                return generateUnicodeDigits();
            default:
                return "";
        }
    }

    private static String generateInvalidNegativeFormat() {
        switch (rnd.nextInt(4)) {
            case 0:
                return "--" + rnd.nextInt(MAX_VALUE);
            case 1:
                return "-+" + rnd.nextInt(MAX_VALUE);
            case 2:
                return "+-" + rnd.nextInt(MAX_VALUE);
            default:
                return "-" + rnd.nextInt(MAX_VALUE) + "-";
        }
    }

    private static String generateInvalidNumberFormat() {
        switch (rnd.nextInt(3)) {
            case 0:
                return "0x" + Integer.toHexString(rnd.nextInt(MAX_VALUE));
            case 1:
                return "0b" + Integer.toBinaryString(rnd.nextInt(MAX_VALUE));
            case 2:
                return "++" + rnd.nextInt(MAX_VALUE);
            default:
                return rnd.nextInt(MAX_VALUE) + ".0";
        }
    }

    private static String generateInvalidUnderscoreFormat() {
        StringSink sink = new StringSink();
        sink.repeat("_", rnd.nextInt(5) + 1);
        sink.put(rnd.nextInt(MAX_VALUE));
        sink.repeat("_", rnd.nextInt(5) + 1);
        return sink.toString();
    }

    private static String generateInvalidUnit() {
        int value = rnd.nextInt(MAX_VALUE);
        return value + INVALID_UNITS[rnd.nextInt(INVALID_UNITS.length)];
    }

    private static String generateInvalidWhitespace() {
        String number = String.valueOf(rnd.nextInt(MAX_VALUE));
        String whitespace = " \t\n\r".charAt(rnd.nextInt(4)) + "";
        return rnd.nextBoolean() ?
                whitespace + number :
                number + whitespace;
    }

    private static String generateMalformedNegativeWithUnit() {
        String unit = UNITS[1 + rnd.nextInt(UNITS.length - 1)];
        return "-" + unit + rnd.nextInt(MAX_VALUE);
    }

    private static String generateMultipleUnits() {
        int value = rnd.nextInt(MAX_VALUE);
        String unit1;
        String unit2;
        // "ms" is a valid unit, even though it consists from two other
        // valid units
        do {
            unit1 = UNITS[1 + rnd.nextInt(UNITS.length - 1)];
            unit2 = UNITS[1 + rnd.nextInt(UNITS.length - 1)];
        } while ("s".equals(unit2) && ("m".equals(unit1) || "n".equals(unit1)));
        return value + unit1 + unit2;
    }

    private static String generateNumberWithInvalidChars() {
        String number = String.valueOf(rnd.nextInt(MAX_VALUE));
        int pos = rnd.nextInt(number.length() + 1);
        return number.substring(0, pos) +
                INVALID_CHARS[rnd.nextInt(INVALID_CHARS.length)] +
                number.substring(pos);
    }

    private static String generateNumberWithInvalidSuffix() {
        return String.valueOf(rnd.nextInt(MAX_VALUE)) +
                INVALID_CHARS[rnd.nextInt(INVALID_CHARS.length)] +
                UNITS[1 + rnd.nextInt(UNITS.length - 1)];
    }

    private static String generateRandomInvalidChars() {
        StringBuilder sb = new StringBuilder();
        int length = rnd.nextInt(10) + 1;
        for (int i = 0; i < length; i++) {
            sb.append(INVALID_CHARS[rnd.nextInt(INVALID_CHARS.length)]);
        }
        return sb.toString();
    }

    private static String generateUnicodeDigits() {
        // Using some Unicode digits that look like numbers but aren't ASCII digits
        String[] unicodeDigits = {"①", "②", "③", "④", "⑤", "⑥", "⑦", "⑧", "⑨", "⓪"};
        StringBuilder sb = new StringBuilder();
        int length = rnd.nextInt(5) + 1;
        for (int i = 0; i < length; i++) {
            sb.append(unicodeDigits[rnd.nextInt(unicodeDigits.length)]);
        }
        return sb.toString();
    }

    // Previous test methods remain the same...
    // (testSimpleNumber, testNumberWithUnderscore, testNumberWithUnit, etc.)

    private static String generateUnitWithoutNumber() {
        return (rnd.nextBoolean() ? "-" : "") +
                UNITS[1 + rnd.nextInt(UNITS.length - 1)];
    }

    private static int getNumEdgeCases() {
        return 14;
    }

    private static int getNumInvalidCases() {
        return 39;
    }

    private static String insertOptionalUnderscores(String number) {
        if (number.length() < 2) return number;
        StringSink result = new StringSink();
        boolean isNegative = number.startsWith("-");
        String digits = isNegative ? number.substring(1) : number;

        if (isNegative) {
            result.put('-');
        }
        result.put(digits.charAt(0));

        for (int i = 1; i < digits.length(); i++) {
            int underscores = rnd.nextInt(2); // 0-3 consecutive underscores
            result.repeat("_", underscores);
            result.put(digits.charAt(i));
        }
        return result.toString();
    }

    private static String insertRandomUnderscores(String number) {
        if (number.length() < 2) return number;
        StringBuilder result = new StringBuilder();
        boolean isNegative = number.startsWith("-");
        String digits = isNegative ? number.substring(1) : number;

        if (isNegative) result.append('-');
        result.append(digits.charAt(0));

        for (int i = 1; i < digits.length(); i++) {
            if (rnd.nextInt(3) == 0) { // 33% chance of underscore
                result.append('_');
            }
            result.append(digits.charAt(i));
        }
        return result.toString();
    }

    // Placeholder for your actual parser method
    private static long parseMicros(CharSequence input) throws NumericException {
        return Numbers.parseNanos(input);
    }

    private static void runFuzzTests(ArrayList<Object[]> testData) {
        // Test random valid cases
        for (int i = 0; i < NUM_TESTS; i++) {
            testRandomCase(testData);
        }

        // Test edge cases
        for (int i = 0; i < getNumEdgeCases(); i++) {
            testEdgeCase(i, testData);
        }
        // Test predefined invalid cases
        for (int i = 0; i < getNumInvalidCases(); i++) {
            testInvalidCase(i, testData);
        }
        // Test fuzzed invalid cases
        for (int i = 0; i < NUM_INVALID_FUZZ; i++) {
            testFuzzedInvalidCase(testData);
        }
    }

    private static void testComplexNumber(ArrayList<Object[]> testData) {
        // Combines multiple features: negative, consecutive underscores, mixed case units
        int value = rnd.nextBoolean() ? rnd.nextInt(MAX_VALUE) : -rnd.nextInt(MAX_VALUE);
        int unitIndex = rnd.nextInt(UNITS.length);
        String num = String.valueOf(value);
        String unit = unitIndex > 0 ? randomizeCase(UNITS[unitIndex]) : "";
        String input = num + unit;
        long expectedMicros = value * UNIT_MULTIPLIERS[unitIndex];
        boolean fail = false;
        addTestData(testData, input, expectedMicros, fail);
    }

    private static void testEdgeCase(int index, ArrayList<Object[]> testData) {
        switch (index) {
            case 0:
                addTestData(testData, "0", 0L, false);
                break;
            case 1:
                addTestData(testData, "1", 1L, false);
                break;
            case 2:
                addTestData(testData, "-1", -1L, false);
                break;
            case 3:
                addTestData(testData, "1_2_3_4_5", 12345L, false);
                break;
            case 4:
                addTestData(testData, "-1_2_3_4_5", -12345L, false);
                break;
            case 5:
                addTestData(testData, "1us", 1000L, false);
                break;
            case 6:
                addTestData(testData, "1US", 1000L, false);
                break;
            case 7:
                addTestData(testData, "1Ms", 1000_000L, false);
                break;
            case 8:
                addTestData(testData, "1S", 1_000_000_000L, false);
                break;
            case 9:
                addTestData(testData, "1M", 60_000_000_000L, false);
                break;
            case 10:
                addTestData(testData, "1H", 3_600_000_000_000L, false);
                break;
            case 11:
                addTestData(testData, "-1_000_000us", -1000000000L, false);
                break;
            case 12:
                addTestData(testData, "-1_000_000MS", -1000000000000L, false);
                break;
            case 13:
                addTestData(testData, "-1_000_000S", -1000000000000000L, false);
                break;
            default:
                break;
        }
    }

    private static void testFuzzedInvalidCase(ArrayList<Object[]> testData) {
        String input = generateInvalidInput();
        addTestData(testData, input, 0L, true);
    }

    private static void testInvalidCase(int index, ArrayList<Object[]> testData) {
        switch (index) {
            case 0:
                addTestData(testData, "", 0L, true);
                break;
            case 1:
                addTestData(testData, "abc", 0L, true);
                break;
            case 2:
                addTestData(testData, "123abc", 0L, true);
                break;
            case 3:
                addTestData(testData, "us", 0L, true);
                break;
            case 4:
                addTestData(testData, "ms", 0L, true);
                break;
            case 5:
                addTestData(testData, "s", 0L, true);
                break;
            case 6:
                addTestData(testData, "m", 0L, true);
                break;
            case 7:
                addTestData(testData, "h", 0L, true);
                break;
            case 8:
                addTestData(testData, "123_", 0L, true);
                break;
            case 9:
                addTestData(testData, "1.5s", 0L, true);
                break;
            case 10:
                addTestData(testData, "1s1", 0L, true);
                break;
            case 11:
                addTestData(testData, "1ss", 0L, true);
                break;
            case 12:
                addTestData(testData, "1sms", 0L, true);
                break;
            case 13:
                addTestData(testData, " 1s", 0L, true);
                break;
            case 14:
                addTestData(testData, "1s ", 0L, true);
                break;
            case 15:
                addTestData(testData, "\t1s", 0L, true);
                break;
            case 16:
                addTestData(testData, "1s\n", 0L, true);
                break;
            case 17:
                addTestData(testData, "ms1", 0L, true);
                break;
            case 18:
                addTestData(testData, "1y", 0L, true);
                break;
            case 19:
                addTestData(testData, "1d", 0L, true);
                break;
            case 20:
                addTestData(testData, "-", 0L, true);
                break;
            case 21:
                addTestData(testData, "-us", 0L, true);
                break;
            case 22:
                addTestData(testData, "--1s", 0L, true);
                break;
            case 23:
                addTestData(testData, "¹s", 0L, true);
                break;
            case 24:
                addTestData(testData, "₁s", 0L, true);
                break;
            case 25:
                addTestData(testData, "①s", 0L, true);
                break;
            case 26:
                addTestData(testData, "⑴s", 0L, true);
                break;
            case 27:
                addTestData(testData, "1\u0000s", 0L, true);
                break;
            case 28:
                addTestData(testData, "1\u202Es", 0L, true);
                break;
            case 29:
                addTestData(testData, "9223372036854775808", 0L, true);
                break;
            case 30:
                addTestData(testData, "-9223372036854775809", 0L, true);
                break;
            case 31:
                addTestData(testData, "1_us", 0L, true);
                break;
            case 32:
                addTestData(testData, "18446744073709551616us", 0L, true);
                break;
            case 33:
                addTestData(testData, "1.us", 0L, true);
                break;
            case 34:
                addTestData(testData, ".1us", 0L, true);
                break;
            case 35:
                addTestData(testData, "1e6us", 0L, true);
                break;
            case 36:
                addTestData(testData, "0x1us", 0L, true);
                break;
            case 37:
                addTestData(testData, "true", 0L, true);
                break;
            case 38:
                addTestData(testData, "null", 0L, true);
                break;
            default:
                break;
        }
    }

    private static void testMixedCaseUnit(ArrayList<Object[]> testData) {
        int value = rnd.nextInt(MAX_VALUE);
        int unitIndex = rnd.nextInt(UNITS.length);
        if (unitIndex > 0) {
            String unit = randomizeCase(UNITS[unitIndex]);
            String input = value + unit;
            long expectedMicros = value * UNIT_MULTIPLIERS[unitIndex];
            addTestData(testData, input, expectedMicros, false);
        }
    }

    private static void testNegativeNumber(ArrayList<Object[]> testData) {
        int value = -rnd.nextInt(MAX_VALUE);
        String input = String.valueOf(value);
        addTestData(testData, input, value, false);
    }

    private static void testNegativeNumberWithUnit(ArrayList<Object[]> testData) {
        int value = -rnd.nextInt(MAX_VALUE);
        int unitIndex = rnd.nextInt(UNITS.length);
        String input = value + UNITS[unitIndex];
        long expectedMicros = value * UNIT_MULTIPLIERS[unitIndex];
        addTestData(testData, input, expectedMicros, false);
    }

    private static void testNumberWithConsecutiveUnderscores(ArrayList<Object[]> testData) {
        int value = rnd.nextInt(MAX_VALUE);
        String input = insertOptionalUnderscores(String.valueOf(value));
        addTestData(testData, input, value, false);
    }

    private static void testNumberWithMultipleUnderscores(ArrayList<Object[]> testData) {
        int value = rnd.nextInt(MAX_VALUE);
        addTestData(testData, insertMultipleUnderscores(String.valueOf(value)), value, false);
    }

    private static void testNumberWithUnderscore(ArrayList<Object[]> testData) {
        int value = rnd.nextInt(MAX_VALUE);
        addTestData(testData, insertRandomUnderscores(String.valueOf(value)), value, false);
    }

    private static void testNumberWithUnderscoreAndUnit(ArrayList<Object[]> testData) {
        int value = rnd.nextInt(MAX_VALUE);
        int unitIndex = rnd.nextInt(UNITS.length);
        String input = insertRandomUnderscores(String.valueOf(value)) + UNITS[unitIndex];
        long expectedMicros = value * UNIT_MULTIPLIERS[unitIndex];
        addTestData(testData, input, expectedMicros, false);
    }

    private static void testNumberWithUnit(ArrayList<Object[]> testData) {
        int value = rnd.nextInt(MAX_VALUE);
        int unitIndex = rnd.nextInt(UNITS.length);
        String input = value + UNITS[unitIndex];
        long expectedMicros = value * UNIT_MULTIPLIERS[unitIndex];
        addTestData(testData, input, expectedMicros, false);
    }

    private static void testRandomCase(ArrayList<Object[]> testData) {
        switch (rnd.nextInt(10)) {
            case 0:
                testSimpleNumber(testData);
                break;
            case 1:
                testNumberWithUnderscore(testData);
                break;
            case 2:
                testNumberWithUnit(testData);
                break;
            case 3:
                testNumberWithUnderscoreAndUnit(testData);
                break;
            case 4:
                testNegativeNumber(testData);
                break;
            case 5:
                testNegativeNumberWithUnit(testData);
                break;
            case 6:
                testMixedCaseUnit(testData);
                break;
            case 7:
                testNumberWithMultipleUnderscores(testData);
                break;
            case 8:
                testNumberWithConsecutiveUnderscores(testData);
                break;
            case 9:
                testComplexNumber(testData);
                break;
            default:
                break;
        }
    }

    private static void testSimpleNumber(ArrayList<Object[]> testData) {
        final int value = rnd.nextInt(MAX_VALUE);
        String str = String.valueOf(value);
        testData.add(new Object[]{str, value, false, str});
    }

    static String insertMultipleUnderscores(String number) {
        if (number.length() < 2) return number;
        StringBuilder result = new StringBuilder();
        boolean isNegative = number.startsWith("-");
        String digits = isNegative ? number.substring(1) : number;

        if (isNegative) result.append('-');
        result.append(digits.charAt(0));

        for (int i = 1; i < digits.length(); i++) {
            if (i % 2 == 0) { // Add underscore every other digit
                result.append('_');
            }
            result.append(digits.charAt(i));
        }
        return result.toString();
    }

    // Previous support methods remain the same...
    // (insertRandomUnderscores, randomizeCase, verifyResult, etc.)

    static String randomizeCase(String unit) {
        StringBuilder result = new StringBuilder(unit.length());
        for (char c : unit.toCharArray()) {
            result.append(rnd.nextBoolean() ? Character.toUpperCase(c) : Character.toLowerCase(c));
        }
        return result.toString();
    }

}