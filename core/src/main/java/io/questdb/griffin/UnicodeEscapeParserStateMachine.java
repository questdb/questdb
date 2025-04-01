package io.questdb.griffin;

import io.questdb.cairo.CairoException;

/**
 * A state machine-based parser that converts strings with Unicode escape sequences
 * into UTF-16 chars and writes them to a Utf16Sink.
 * <p>
 * This implementation:
 * 1. Uses a single long to encode all state information
 * 2. Processes input character by character without looking ahead
 * 3. Supports shortened Unicode escapes (e.g., \\u41 instead of \u00041)
 * 4. Supports escaped Unicode sequences (\\u)
 * 5. Validates surrogate pairs
 * 6. Uses an int buffer instead of StringBuilder for hex digits
 */
public class UnicodeEscapeParserStateMachine {
    // For hex digit parsing - high 4 bits store the digit count (0-4), low 28 bits store the hex value
    private static final int HEX_BUFFER_DIGIT_COUNT_SHIFT = 28;
    private static final int HEX_BUFFER_VALUE_MASK = 0x0FFFFFFF;
    // High surrogate is stored in bits 16-31 (16 bits)
    private static final long HIGH_SURROGATE_SHIFT = 16;
    private static final long HIGH_SURROGATE_MASK = 0xFFFFL << HIGH_SURROGATE_SHIFT;
    private static final int STATE_BACKSLASH_SEEN = 1;
    // State mask for the parser state portion
    private static final long STATE_MASK = 0xFL;
    // State constants - encoded in the lowest 4 bits of the state long
    private static final int STATE_NORMAL = 0;
    private static final int STATE_UNICODE_DIGITS = 3;
    private static final int STATE_UNICODE_START = 2;
    // Surrogate pair tracking flag is stored in bit 32
    private static final long WAITING_FOR_LOW_SURROGATE_FLAG = 1L << 32;

    /**
     * Parses a string that may contain Unicode escape sequences and writes the
     * resulting UTF-16 chars to the sink.
     *
     * @param input the string to parse
     * @throws io.questdb.cairo.CairoException if the input contains invalid Unicode sequences
     */
    public static void parse(CharSequence input, Utf16Sink sink) {
        // Initial state: NORMAL, no high surrogate, not waiting for low surrogate
        long state = STATE_NORMAL;
        // Initialize hex buffer: 0 digits, 0 value
        int hexBuffer = 0;

        for (int i = 0, len = input.length(); i < len; i++) {
            char c = input.charAt(i);
            switch ((int) (state & STATE_MASK)) {
                case STATE_NORMAL:
                    if (c == '\\') {
                        state = (state & ~STATE_MASK) | STATE_BACKSLASH_SEEN;
                    } else {
                        state = processRegularChar(state, c, i, sink);
                    }
                    break;
                case STATE_BACKSLASH_SEEN:
                    switch (c) {
                        case '\\':
                            // Escaped backslash
                            state = processRegularChar(state & ~STATE_MASK, '\\', i, sink);
                            break;
                        case 'u':
                            // Start of Unicode escape
                            hexBuffer = 0; // Reset hex buffer (0 digits, 0 value)
                            state = (state & ~STATE_MASK) | STATE_UNICODE_START;
                            break;
                        default:
                            // Backslash followed by something else - treat as literal chars
                            state = processRegularChar(state & ~STATE_MASK, '\\', i, sink);
                            state = processRegularChar(state, c, i, sink);
                            break;
                    }
                    break;
                case STATE_UNICODE_START:
                    if (isHexDigit(c)) {
                        // First hex digit
                        int digitValue = hexDigitValue(c);
                        // Set digit count to 1, value to the digit value
                        hexBuffer = (1 << HEX_BUFFER_DIGIT_COUNT_SHIFT) | digitValue;
                        state = (state & ~STATE_MASK) | STATE_UNICODE_DIGITS;
                        break;
                    }
                    throw CairoException.nonCritical().position(i).put("Expected hex digit after \\u");
                case STATE_UNICODE_DIGITS:
                    int digitCount = hexBuffer >>> HEX_BUFFER_DIGIT_COUNT_SHIFT;

                    if (isHexDigit(c) && digitCount < 4) {
                        // Collect up to 4 hex digits
                        int digitValue = hexDigitValue(c);
                        int currentValue = hexBuffer & HEX_BUFFER_VALUE_MASK;
                        // Shift existing value left 4 bits and add new digit
                        int newValue = (currentValue << 4) | digitValue;
                        // Increment digit count
                        hexBuffer = ((digitCount + 1) << HEX_BUFFER_DIGIT_COUNT_SHIFT) | newValue;
                        break;
                    }

                    // Either we have 4 digits or encountered a non-hex character
                    if (digitCount < 2) {
                        throw CairoException.nonCritical().position(i)
                                .put("Unicode escape needs at least 2 hex digits");
                    }

                    // Process the collected hex digits
                    int codePoint = hexBuffer & HEX_BUFFER_VALUE_MASK;
                    state = processUnicodeChar(state & ~STATE_MASK, (char) codePoint, i, sink);

                    // For an edge case with \\uXXXX\\u at the end of input
                    // The first escape is processed and then we see \\u at the end
                    if (c == '\\' && i == input.length() - 2) {
                        // We're at the backslash of a \\u at the end, this will be caught in finalize()
                        state = (state & ~STATE_MASK) | STATE_BACKSLASH_SEEN;
                        break;
                    } else if (c == 'u' && i == len - 1 && i > 0 && input.charAt(i - 1) == '\\') {
                        // We're at the 'u' of a \\u at the end, this will be caught in finalize()
                        state = (state & ~STATE_MASK) | STATE_UNICODE_START;
                        break;
                    }

                    // Go back to process the current character again in NORMAL state
                    state = state & ~STATE_MASK;
                    if (c == '\\') {
                        state = (state & ~STATE_MASK) | STATE_BACKSLASH_SEEN;
                    } else {
                        state = processRegularChar(state, c, i, sink);
                    }
                    break;
            }
        }

        // Check for incomplete sequences at the end
        finalize(state, input.length(), hexBuffer, sink);
    }

    /**
     * Get a description of a character for error messages.
     *
     * @param c the character to describe
     * @return a human-readable description of the character
     */
    private static String charDescription(char c) {
        if (c < 32 || c > 126) {
            return String.format("U+%04X", (int) c);
        } else {
            return "'" + c + "'";
        }
    }

    /**
     * Finalize the parsing, handling any incomplete sequences.
     *
     * @param state     the current parser state
     * @param position  position at the end of input for error reporting
     * @param hexBuffer the current hex buffer value
     * @throws IllegalArgumentException if there's an incomplete sequence at the end
     */
    private static void finalize(long state, int position, int hexBuffer, Utf16Sink sink) {
        long currentState = state & STATE_MASK;
        boolean waitingForLowSurrogate = (state & WAITING_FOR_LOW_SURROGATE_FLAG) != 0;

        switch ((int) currentState) {
            case STATE_BACKSLASH_SEEN:
                throw CairoException.nonCritical().position(position).put("Incomplete escape sequence at the end of input");
            case STATE_UNICODE_START:
                throw CairoException.nonCritical().position(position).put("Incomplete Unicode escape sequence at the end of input");
            case STATE_UNICODE_DIGITS:
                int digitCount = hexBuffer >>> HEX_BUFFER_DIGIT_COUNT_SHIFT;
                if (digitCount < 2) {
                    throw CairoException.nonCritical().position(position).put("Unicode escape needs at least 2 hex digits at the end of input");
                }

                int codePoint = hexBuffer & HEX_BUFFER_VALUE_MASK;
                char decodedChar = (char) codePoint;

                // Handle surrogate validation when we already have a high surrogate
                if (waitingForLowSurrogate) {
                    if (Character.isLowSurrogate(decodedChar)) {
                        // This is fine - we have a valid surrogate pair at the end
                        sink.accept((char) ((state & HIGH_SURROGATE_MASK) >> HIGH_SURROGATE_SHIFT));
                        sink.accept(decodedChar);
                        // Clear the waiting flag since we've handled it
                        waitingForLowSurrogate = false;
                    } else {
                        throw CairoException.nonCritical().position(position)
                                .put("Expected low surrogate but got " + charDescription(decodedChar) + " from Unicode escape at the end of input");
                    }
                } else if (Character.isHighSurrogate(decodedChar)) {
                    // Don't accept a high surrogate at the end - it would be dangling
                    throw CairoException.nonCritical().position(position)
                            .put("Dangling high surrogate from Unicode escape at the end of input");
                } else if (Character.isLowSurrogate(decodedChar)) {
                    throw CairoException.nonCritical().position(position)
                            .put("Unexpected low surrogate from Unicode escape without preceding high surrogate at the end of input");
                } else {
                    // Regular character
                    sink.accept(decodedChar);
                }
        }

        // Check for dangling high surrogate
        if (waitingForLowSurrogate) {
            throw CairoException.nonCritical().position(position).put("Dangling high surrogate at the end of input");
        }
    }

    /**
     * Converts a hex character to its integer value.
     *
     * @param c the hex character
     * @return the integer value (0-15)
     */
    private static int hexDigitValue(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        } else if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        } else if (c >= 'A' && c <= 'F') {
            return c - 'A' + 10;
        }
        throw new IllegalArgumentException("Not a hex digit: " + c);
    }

    /**
     * Checks if a character is a valid hexadecimal digit.
     *
     * @param c the character to check
     * @return true if the character is a hex digit (0-9, a-f, A-F)
     */
    private static boolean isHexDigit(char c) {
        return (c >= '0' && c <= '9') ||
                (c >= 'a' && c <= 'f') ||
                (c >= 'A' && c <= 'F');
    }

    /**
     * Process a regular (non-escape-sequence) character.
     *
     * @param state    the current parser state
     * @param c        the character to process
     * @param position position in the input string for error reporting
     * @return the new parser state
     * @throws IllegalArgumentException if the character creates an invalid surrogate sequence
     */
    private static long processRegularChar(long state, char c, int position, Utf16Sink sink) {
        boolean waitingForLowSurrogate = (state & WAITING_FOR_LOW_SURROGATE_FLAG) != 0;
        char highSurrogate = (char) ((state & HIGH_SURROGATE_MASK) >> HIGH_SURROGATE_SHIFT);

        if (waitingForLowSurrogate) {
            if (Character.isLowSurrogate(c)) {
                // Complete surrogate pair
                int codePoint = Character.toCodePoint(highSurrogate, c);

                // Output the combined character
                if (codePoint >= 0x10000) {
                    // Write the surrogate pair components
                    sink.accept(highSurrogate);
                    sink.accept(c);
                } else {
                    // Shouldn't happen with valid surrogate pairs, but just in case
                    sink.accept((char) codePoint);
                }

                // Clear surrogate tracking
                return state & ~WAITING_FOR_LOW_SURROGATE_FLAG & ~HIGH_SURROGATE_MASK;
            } else {
                throw new IllegalArgumentException(
                        "Expected low surrogate but got " + charDescription(c) + " at position " + position);
            }
        } else if (Character.isHighSurrogate(c)) {
            // Start of surrogate pair
            return (state & ~HIGH_SURROGATE_MASK) |
                    ((long) c << HIGH_SURROGATE_SHIFT) |
                    WAITING_FOR_LOW_SURROGATE_FLAG;
        } else if (Character.isLowSurrogate(c)) {
            throw new IllegalArgumentException(
                    "Unexpected low surrogate without preceding high surrogate at position " + position);
        } else {
            // Regular character
            sink.accept(c);
            return state;
        }
    }

    /**
     * Process a character decoded from a Unicode escape sequence.
     *
     * @param state    the current parser state
     * @param c        the character decoded from the escape sequence
     * @param position position in the input string for error reporting
     * @return the new parser state
     * @throws IllegalArgumentException if the character creates an invalid surrogate sequence
     */
    private static long processUnicodeChar(long state, char c, int position, Utf16Sink sink) {
        boolean waitingForLowSurrogate = (state & WAITING_FOR_LOW_SURROGATE_FLAG) != 0;
        char highSurrogate = (char) ((state & HIGH_SURROGATE_MASK) >> HIGH_SURROGATE_SHIFT);

        if (waitingForLowSurrogate) {
            if (Character.isLowSurrogate(c)) {
                // Complete surrogate pair
                // Output the combined character
                sink.accept(highSurrogate);
                sink.accept(c);

                // Clear surrogate tracking
                return state & ~WAITING_FOR_LOW_SURROGATE_FLAG & ~HIGH_SURROGATE_MASK;
            } else {
                throw new IllegalArgumentException(
                        "Expected low surrogate but got " + charDescription(c) +
                                " from Unicode escape at position " + position);
            }
        } else if (Character.isHighSurrogate(c)) {
            // Start of surrogate pair
            return (state & ~HIGH_SURROGATE_MASK) |
                    ((long) c << HIGH_SURROGATE_SHIFT) |
                    WAITING_FOR_LOW_SURROGATE_FLAG;
        } else if (Character.isLowSurrogate(c)) {
            throw new IllegalArgumentException(
                    "Unexpected low surrogate from Unicode escape without preceding high surrogate at position " + position);
        } else {
            // Regular character
            sink.accept(c);
            return state;
        }
    }

    /**
     * Interface for a sink that accepts UTF-16 chars.
     */
    public interface Utf16Sink {
        void accept(char c);
    }
}