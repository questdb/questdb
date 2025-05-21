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

package io.questdb.test.griffin;

import io.questdb.griffin.UnicodeParser;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * JUnit test suite for UnicodeEscapeParserStateMachine.
 * Uses parameterized tests to dynamically generate test cases from existing test data.
 */
@RunWith(Parameterized.class)
public class UnicodeParserFuzzTest {

    // Maximum length of generated test strings
    private static final int MAX_STRING_LENGTH = 100;
    // Seed for reproducible tests
    private static final long SEED = 12345L;
    // Test case information
    private final String description;
    private final boolean expectException;
    private final String expectedOutput;
    private final String input;

    /**
     * Constructor for parameterized tests.
     */
    public UnicodeParserFuzzTest(String description, String input, String expectedOutput, boolean expectException) {
        this.description = description;
        this.input = input;
        this.expectedOutput = expectedOutput;
        this.expectException = expectException;
    }

    /**
     * Generates the test parameters from both basic and fuzz tests.
     */
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> testCases = new ArrayList<>();

        // Add basic test cases
        addBasicTestCases(testCases);

        // Add fuzz test cases
        addFuzzTestCases(testCases);

        return testCases;
    }

    /**
     * The actual test that runs for each test case.
     */
    @Test
    public void testUnicodeParser() {
        if (expectException) {
            try {
                String actualOutput = parseString(input);
                Assert.fail("Expected exception but got: " + escapeForDisplay(actualOutput));
            } catch (Exception e) {
                // Expected behavior - test passes
            }
        } else {
            String actualOutput = parseString(input);
            Assert.assertEquals(
                    "Test failed for: " + description + "\nInput: " + escapeForDisplay(input),
                    expectedOutput,
                    actualOutput
            );
        }
    }

    /**
     * Add predefined basic test cases to the test suite.
     */
    private static void addBasicTestCases(List<Object[]> testCases) {
        // Basic test cases
        addTestCase(testCases, "Empty string", "", "");
        addTestCase(testCases, "Simple ASCII", "Hello, World!", "Hello, World!");
        addTestCase(testCases, "Basic unicode escape", "\\u0041\\u0042\\u0043", "ABC");
        addTestCase(testCases, "Shorter unicode escapes", "\\u41\\u42\\u43", "ABC");
        addTestCase(testCases, "Single digit unicode", "\\u7\\uA\\uF", null);  // Should throw - need at least 2 digits
        addTestCase(testCases, "Escaped unicode sequences", "\\\\u0041", "\\u0041");
        addTestCase(testCases, "Mixed escaping", "\\\\\\u0041", "\\A");
        addTestCase(testCases, "Double escaping", "\\\\\\\\u0041", "\\\\u0041");
        addTestCase(testCases, "Control characters", "\\u0000\\u001F\\u007F", "\u0000\u001F\u007F");
        addTestCase(testCases, "Invalid sequences", "\\uGGGG", null); // Should throw an exception
        addTestCase(testCases, "Incomplete unicode", "\\u", null); // Should throw an exception
        addTestCase(testCases, "Truncated unicode", "\\u123", "ģ");
        addTestCase(testCases, "Boundary case BMP max", "\\uFFFF", "\uFFFF");
        addTestCase(testCases, "Unicode at end", "abc\\u0041", "abcA");
        addTestCase(testCases, "Backslash at end", "abc\\", null); // Should throw an exception
        addTestCase(testCases, "Multiple backslashes", "\\\\\\\\", "\\\\");
        addTestCase(testCases, "Escaped backslash at end", "\\\\", "\\");
        addTestCase(testCases, "Surrogate pair", "\\uD83D\\uDE00", "😀");
        addTestCase(testCases, "Dangling high surrogate", "\\uD83D", null);  // Should throw an exception
        addTestCase(testCases, "Orphaned low surrogate", "\\uDE00", null);  // Should throw an exception
        addTestCase(testCases, "Surrogate pair mixed with text", "Hello \\uD83D\\uDE00 World", "Hello 😀 World");
    }

    /**
     * Add randomly generated fuzz test cases to the test suite.
     */
    private static void addFuzzTestCases(List<Object[]> testCases) {
        Random random = new Random(SEED);

        for (int i = 0; i < 1000; i++) {
            // Generate a random test string with various escape patterns
            String testString = generateRandomTestString(random);
            ExpectedResult expected = calculateExpectedOutput(testString);

            addTestCase(testCases, "Fuzz test #" + (i + 1), testString, expected.output);
        }
    }

    /**
     * Helper method to add a test case to the list.
     */
    private static void addTestCase(List<Object[]> testCases, String description, String input, String expectedOutput) {
        boolean expectException = (expectedOutput == null);
        testCases.add(new Object[]{description, input, expectedOutput, expectException});
    }

    /**
     * Calculate the expected output for a given input string.
     * Returns a result object with null output if an exception is expected.
     */
    private static ExpectedResult calculateExpectedOutput(String input) {
        if (input == null || input.isEmpty()) {
            return ExpectedResult.success("");
        }

        StringBuilder result = new StringBuilder();

        // Parser state machine variables
        State state = State.NORMAL;
        StringBuilder hexBuffer = new StringBuilder(4);
        boolean waitingForLowSurrogate = false;
        char highSurrogate = 0;

        try {
            for (int i = 0; i < input.length(); i++) {
                char c = input.charAt(i);

                switch (state) {
                    case NORMAL:
                        if (c == '\\') {
                            state = State.BACKSLASH_SEEN;
                        } else {
                            // Regular character handling with surrogate pair validation
                            if (waitingForLowSurrogate) {
                                if (Character.isLowSurrogate(c)) {
                                    // Valid surrogate pair
                                    int codePoint = Character.toCodePoint(highSurrogate, c);
                                    result.appendCodePoint(codePoint);
                                    waitingForLowSurrogate = false;
                                } else {
                                    // Expected low surrogate but got something else
                                    return ExpectedResult.exception();
                                }
                            } else if (Character.isHighSurrogate(c)) {
                                // Start of surrogate pair
                                highSurrogate = c;
                                waitingForLowSurrogate = true;
                            } else if (Character.isLowSurrogate(c)) {
                                // Orphaned low surrogate
                                return ExpectedResult.exception();
                            } else {
                                // Regular character
                                result.append(c);
                            }
                        }
                        break;

                    case BACKSLASH_SEEN:
                        if (c == '\\') {
                            // Escaped backslash
                            result.append('\\');
                            state = State.NORMAL;
                        } else if (c == 'u') {
                            // Start of Unicode escape
                            hexBuffer.setLength(0);
                            state = State.UNICODE_START;
                        } else {
                            // Backslash followed by anything else
                            result.append('\\').append(c);
                            state = State.NORMAL;
                        }
                        break;

                    case UNICODE_START:
                        if (isHexDigit(c)) {
                            // First hex digit
                            hexBuffer.append(c);
                            state = State.UNICODE_DIGITS;
                        } else {
                            // Expected hex digit but got something else
                            return ExpectedResult.exception();
                        }
                        break;

                    case UNICODE_DIGITS:
                        if (isHexDigit(c) && hexBuffer.length() < 4) {
                            // Collect up to 4 hex digits
                            hexBuffer.append(c);
                        } else {
                            // Either we have 4 digits or encountered a non-hex character

                            // Must have at least 2 hex digits
                            if (hexBuffer.length() < 2) {
                                return ExpectedResult.exception();
                            }

                            // Process the Unicode escape
                            try {
                                int codePoint = Integer.parseInt(hexBuffer.toString(), 16);
                                char decodedChar = (char) codePoint;

                                // Handle surrogate pair validation
                                if (waitingForLowSurrogate) {
                                    if (Character.isLowSurrogate(decodedChar)) {
                                        // Valid surrogate pair
                                        int fullCodePoint = Character.toCodePoint(highSurrogate, decodedChar);
                                        result.appendCodePoint(fullCodePoint);
                                        waitingForLowSurrogate = false;
                                    } else {
                                        // Expected low surrogate but got something else
                                        return ExpectedResult.exception();
                                    }
                                } else if (Character.isHighSurrogate(decodedChar)) {
                                    // Start of surrogate pair
                                    highSurrogate = decodedChar;
                                    waitingForLowSurrogate = true;
                                } else if (Character.isLowSurrogate(decodedChar)) {
                                    // Orphaned low surrogate
                                    return ExpectedResult.exception();
                                } else {
                                    // Regular character
                                    result.append(decodedChar);
                                }

                            } catch (NumberFormatException e) {
                                return ExpectedResult.exception();
                            }

                            // Process current character in normal state
                            state = State.NORMAL;
                            if (hexBuffer.length() == 4 || !isHexDigit(c)) {
                                // Need to reprocess the current character if it's not a hex digit,
                                // or we've already collected 4 digits
                                i--; // Back up one character
                            }
                        }
                        break;
                }
            }

            // Check for incomplete sequences at the end
            switch (state) {
                case BACKSLASH_SEEN:
                case UNICODE_START:
                    return ExpectedResult.exception();

                case UNICODE_DIGITS:
                    // Process any remaining hex digits at the end
                    if (hexBuffer.length() < 2) {
                        return ExpectedResult.exception();
                    }

                    try {
                        int codePoint = Integer.parseInt(hexBuffer.toString(), 16);
                        char decodedChar = (char) codePoint;

                        // Handle surrogate pair validation
                        if (waitingForLowSurrogate) {
                            if (Character.isLowSurrogate(decodedChar)) {
                                // Valid surrogate pair
                                int fullCodePoint = Character.toCodePoint(highSurrogate, decodedChar);
                                result.appendCodePoint(fullCodePoint);
                                waitingForLowSurrogate = false;
                            } else {
                                // Expected low surrogate but got something else
                                return ExpectedResult.exception();
                            }
                        } else if (Character.isHighSurrogate(decodedChar)) {
                            // Dangling high surrogate at the end
                            return ExpectedResult.exception();
                        } else if (Character.isLowSurrogate(decodedChar)) {
                            // Orphaned low surrogate
                            return ExpectedResult.exception();
                        } else {
                            // Regular character
                            result.append(decodedChar);
                        }

                    } catch (NumberFormatException e) {
                        return ExpectedResult.exception();
                    }
                    break;
            }

            // Check for dangling high surrogate at the end
            if (waitingForLowSurrogate) {
                return ExpectedResult.exception();
            }

            return ExpectedResult.success(result.toString());

        } catch (Exception e) {
            // Any other exception means we should expect an exception
            return ExpectedResult.exception();
        }
    }

    /**
     * Escape a string for display in test output.
     */
    private static String escapeForDisplay(String s) {
        if (s == null) {
            return "null";
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < 32 || c > 126) {
                sb.append(String.format("\\u%04x", (int) c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Generate a random ASCII string.
     */
    private static String generateRandomAscii(Random random, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            // Printable ASCII range (32-126)
            sb.append((char) (32 + random.nextInt(95)));
        }
        return sb.toString();
    }

    /**
     * Generate a random hex string of specified length.
     */
    private static String generateRandomHex(Random random, int length) {
        StringBuilder sb = new StringBuilder(length);
        String hexChars = "0123456789abcdefABCDEF";
        for (int i = 0; i < length; i++) {
            sb.append(hexChars.charAt(random.nextInt(hexChars.length())));
        }
        return sb.toString();
    }

    /**
     * Generate a random test string with various Unicode escape patterns.
     */
    private static String generateRandomTestString(Random random) {
        int length = random.nextInt(MAX_STRING_LENGTH) + 1;
        StringBuilder sb = new StringBuilder(length);

        while (sb.length() < length) {
            int patternType = random.nextInt(10);

            switch (patternType) {
                case 0: // Regular ASCII
                    sb.append(generateRandomAscii(random, 1 + random.nextInt(5)));
                    break;
                case 1: // Standard Unicode escape
                    sb.append("\\u").append(generateRandomHex(random, 4));
                    break;
                case 2: // Short Unicode escape (1-3 digits)
                    sb.append("\\u").append(generateRandomHex(random, 1 + random.nextInt(3)));
                    break;
                case 3: // Escaped Unicode
                    sb.append("\\\\u").append(generateRandomHex(random, 4));
                    break;
                case 4: // Multiple backslashes
                    int numBackslashes = 1 + random.nextInt(4);
                    sb.append("\\".repeat(numBackslashes));
                    break;
                case 5: // Invalid hex in Unicode
                    if (random.nextBoolean()) {
                        // Invalid character
                        sb.append("\\u").append(generateRandomHex(random, random.nextInt(4)))
                                .append((char) ('G' + random.nextInt(20)));
                    } else {
                        // No hex digits
                        sb.append("\\u");
                    }
                    // These will make the string invalid, so we should return early
                    return sb.toString();
                case 6: // High/low surrogate pair
                    // High surrogate range: 0xD800-0xDBFF
                    int high = 0xD800 + random.nextInt(0x400);
                    // Low surrogate range: 0xDC00-0xDFFF
                    int low = 0xDC00 + random.nextInt(0x400);

                    sb.append("\\u").append(Integer.toHexString(high))
                            .append("\\u").append(Integer.toHexString(low));
                    break;
                case 7: // Character at BMP boundaries
                    int boundaryType = random.nextInt(3);
                    if (boundaryType == 0) {
                        sb.append("\\u0000"); // Null character
                    } else if (boundaryType == 1) {
                        sb.append("\\uFFFF"); // Max BMP
                    } else {
                        // Random control character
                        int controlChar = random.nextInt(32);
                        sb.append("\\u").append(String.format("%04x", controlChar));
                    }
                    break;
                case 8: // Mixed normal and escaped backslashes with Unicode
                    if (random.nextBoolean()) {
                        sb.append("\\\\\\u").append(generateRandomHex(random, 4));
                    } else {
                        sb.append("\\u").append(generateRandomHex(random, 2)).append("\\\\");
                    }
                    break;
                case 9: // Just add a backslash at the end
                    if (sb.length() == length - 1) {
                        sb.append("\\");
                        // This will make the string invalid, so we should return early
                        return sb.toString();
                    } else {
                        sb.append("\\\\");
                    }
                    break;
            }
        }

        // Trim to exact length
        if (sb.length() > length) {
            return sb.substring(0, length);
        }

        return sb.toString();
    }

    /**
     * Checks if a character is a valid hex digit.
     */
    private static boolean isHexDigit(char c) {
        return (c >= '0' && c <= '9') ||
                (c >= 'a' && c <= 'f') ||
                (c >= 'A' && c <= 'F');
    }

    /**
     * Parse a string using our parser and return the result.
     */
    private static String parseString(String input) {
        StringSink sink = new StringSink();

        // Parse the input
        UnicodeParser.parse(input, 0, sink);

        return sink.toString();
    }

    // Parser states for expected result calculation
    private enum State {
        NORMAL, BACKSLASH_SEEN, UNICODE_START, UNICODE_DIGITS
    }

    /**
     * Class to hold the expected result of parsing.
     */
    private static class ExpectedResult {
        final String output;  // null means exception expected

        ExpectedResult(String output) {
            this.output = output;
        }

        static ExpectedResult exception() {
            return new ExpectedResult(null);
        }

        static ExpectedResult success(String output) {
            return new ExpectedResult(output);
        }
    }
}