package io.questdb.test.griffin;

import io.questdb.griffin.UnicodeEscapeParserStateMachine;

import java.util.Random;

/**
 * Comprehensive fuzz test for the UnicodeEscapeParserStateMachine.
 * This test generates random input strings with various Unicode escape patterns
 * and verifies that the parser handles them correctly.
 */
public class UnicodeParserFuzzTest {
    // Seed for reproducible tests
    private static final long SEED = 12345L;
    // Number of test cases to generate
    private static final int NUM_TEST_CASES = 1000;
    // Maximum length of generated test strings
    private static final int MAX_STRING_LENGTH = 100;

    // Random instance with fixed seed for reproducibility
    private static final Random random = new Random(SEED);

    // Test stats
    private static int totalTests = 0;
    private static int passedTests = 0;
    private static int failedTests = 0;

    public static void main(String[] args) {
        System.out.println("Starting Unicode Parser Fuzz Test");
        System.out.println("=================================");
        System.out.println("Seed: " + SEED);
        System.out.println("Test cases: " + NUM_TEST_CASES);
        System.out.println();

        runBasicTests();
        runRandomFuzzTests();

        // Print test summary
        System.out.println("\nTest Summary:");
        System.out.println("=============");
        System.out.println("Total tests: " + totalTests);
        System.out.println("Passed: " + passedTests);
        System.out.println("Failed: " + failedTests);

        if (failedTests == 0) {
            System.out.println("\nALL TESTS PASSED!");
        } else {
            System.out.println("\nTEST FAILURES DETECTED!");
            System.exit(1);
        }
    }

    /**
     * Run a set of basic predefined test cases to catch common issues.
     */
    private static void runBasicTests() {
        System.out.println("Running basic tests...");

        testCase("Empty string", "", "");
        testCase("Simple ASCII", "Hello, World!", "Hello, World!");
        testCase("Basic unicode escape", "\\u0041\\u0042\\u0043", "ABC");
        testCase("Shorter unicode escapes", "\\u41\\u42\\u43", "ABC");
        testCase("Single digit unicode", "\\u7\\uA\\uF", null);  // Should throw - need at least 2 digits
        testCase("Escaped unicode sequences", "\\\\u0041", "\\u0041");
        testCase("Mixed escaping", "\\\\\\u0041", "\\A");
        testCase("Double escaping", "\\\\\\\\u0041", "\\\\u0041");
        testCase("Control characters", "\\u0000\\u001F\\u007F", "\u0000\u001F\u007F");
        testCase("Invalid sequences", "\\uGGGG", null); // Should throw an exception
        testCase("Incomplete unicode", "\\u", null); // Should throw an exception
        testCase("Truncated unicode", "\\u123", "\u0123");
        testCase("Boundary case BMP max", "\\uFFFF", "\uFFFF");
        testCase("Unicode at end", "abc\\u0041", "abcA");
        testCase("Backslash at end", "abc\\", null); // Should throw an exception
        testCase("Multiple backslashes", "\\\\\\\\", "\\\\");
        testCase("Escaped backslash at end", "\\\\", "\\");
        testCase("Surrogate pair", "\\uD83D\\uDE00", "😀");
        testCase("Dangling high surrogate", "\\uD83D", null);  // Should throw an exception
        testCase("Orphaned low surrogate", "\\uDE00", null);  // Should throw an exception
        testCase("Surrogate pair mixed with text", "Hello \\uD83D\\uDE00 World", "Hello 😀 World");
    }

    /**
     * Generate and run random fuzz tests.
     */
    private static void runRandomFuzzTests() {
        System.out.println("\nRunning random fuzz tests...");

        for (int i = 0; i < NUM_TEST_CASES; i++) {
            // Generate a random test string with various escape patterns
            String testString = generateRandomTestString();
            ExpectedResult expected = calculateExpectedOutput(testString);

            try {
                testCase("Fuzz test #" + (i + 1), testString, expected.output);
            } catch (Exception e) {
                // If we expect an exception but don't know which one specifically
                if (expected.output == null) {
                    // This is fine, we expected an exception
                    System.out.println("  ✓ Exception correctly thrown: " + e.getClass().getSimpleName());
                    totalTests++;
                    passedTests++;
                } else {
                    // Unexpected exception
                    System.out.println("  ✗ Unexpected exception: " + e.getMessage());
                    e.printStackTrace(System.out);
                    totalTests++;
                    failedTests++;
                }
            }
        }
    }

    /**
     * Generate a random test string with various Unicode escape patterns.
     */
    private static String generateRandomTestString() {
        int length = random.nextInt(MAX_STRING_LENGTH) + 1;
        StringBuilder sb = new StringBuilder(length);

        while (sb.length() < length) {
            int patternType = random.nextInt(10);

            switch (patternType) {
                case 0: // Regular ASCII
                    sb.append(generateRandomAscii(1 + random.nextInt(5)));
                    break;
                case 1: // Standard Unicode escape
                    sb.append("\\u").append(generateRandomHex(4));
                    break;
                case 2: // Short Unicode escape (1-3 digits)
                    sb.append("\\u").append(generateRandomHex(1 + random.nextInt(3)));
                    break;
                case 3: // Escaped Unicode
                    sb.append("\\\\u").append(generateRandomHex(4));
                    break;
                case 4: // Multiple backslashes
                    int numBackslashes = 1 + random.nextInt(4);
                    for (int i = 0; i < numBackslashes; i++) {
                        sb.append("\\");
                    }
                    break;
                case 5: // Invalid hex in Unicode
                    if (random.nextBoolean()) {
                        // Invalid character
                        sb.append("\\u").append(generateRandomHex(random.nextInt(4)))
                                .append((char)('G' + random.nextInt(20)));
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
                        sb.append("\\\\\\u").append(generateRandomHex(4));
                    } else {
                        sb.append("\\u").append(generateRandomHex(2)).append("\\\\");
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
     * Generate a random ASCII string.
     */
    private static String generateRandomAscii(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            // Printable ASCII range (32-126)
            sb.append((char)(32 + random.nextInt(95)));
        }
        return sb.toString();
    }

    /**
     * Generate a random hex string of specified length.
     */
    private static String generateRandomHex(int length) {
        StringBuilder sb = new StringBuilder(length);
        String hexChars = "0123456789abcdefABCDEF";
        for (int i = 0; i < length; i++) {
            sb.append(hexChars.charAt(random.nextInt(hexChars.length())));
        }
        return sb.toString();
    }

    /**
     * Class to hold the expected result of parsing.
     */
    private static class ExpectedResult {
        final String output;  // null means exception expected

        ExpectedResult(String output) {
            this.output = output;
        }

        static ExpectedResult success(String output) {
            return new ExpectedResult(output);
        }

        static ExpectedResult exception() {
            return new ExpectedResult(null);
        }
    }

    enum State {
        NORMAL, BACKSLASH_SEEN, UNICODE_START, UNICODE_DIGITS
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
                                // Need to reprocess the current character if it's not a hex digit
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
     * Checks if a character is a valid hex digit.
     */
    private static boolean isHexDigit(char c) {
        return (c >= '0' && c <= '9') ||
                (c >= 'a' && c <= 'f') ||
                (c >= 'A' && c <= 'F');
    }

    /**
     * Test a specific case and report results.
     */
    private static void testCase(String description, String input, String expectedOutput) {
        System.out.println("Test: " + description);
        System.out.println("  Input: " + escapeForDisplay(input));

        totalTests++;

        if (expectedOutput == null) {
            // We expect an exception for this input
            try {
                String actualOutput = parseString(input);
                System.out.println("  ✗ Expected exception, but got: " + escapeForDisplay(actualOutput));
                failedTests++;
            } catch (Exception e) {
                System.out.println("  ✓ Exception correctly thrown: " + e.getClass().getSimpleName());
                passedTests++;
            }
        } else {
            try {
                String actualOutput = parseString(input);
                System.out.println("  Expected: " + escapeForDisplay(expectedOutput));
                System.out.println("  Actual: " + escapeForDisplay(actualOutput));

                if (expectedOutput.equals(actualOutput)) {
                    System.out.println("  ✓ PASS");
                    passedTests++;
                } else {
                    System.out.println("  ✗ FAIL");
                    failedTests++;
                }
            } catch (Exception e) {
                System.out.println("  ✗ Unexpected exception: " + e.getMessage());
                e.printStackTrace(System.out);
                failedTests++;
            }
        }

        System.out.println();
    }

    /**
     * Parse a string using our parser and return the result.
     */
    private static String parseString(String input) {
        final StringBuilder result = new StringBuilder();

        // Create a sink that collects the output
        UnicodeEscapeParserStateMachine.Utf16Sink testSink = result::append;

        // Parse the input
        UnicodeEscapeParserStateMachine parser = new UnicodeEscapeParserStateMachine(testSink);
        parser.parse(input);

        return result.toString();
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
}

