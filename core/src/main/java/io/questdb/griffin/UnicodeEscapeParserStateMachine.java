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

package io.questdb.griffin;

/**
 * A state machine-based parser that converts strings with Unicode escape sequences
 * into UTF-16 chars and writes them to a Utf16Sink.
 * <p>
 * This implementation:
 * 1. Processes input character by character without looking ahead
 * 2. Supports shortened Unicode escapes (e.g., \\u41 instead of A)
 * 3. Supports escaped Unicode sequences (\\u)
 * 4. Validates surrogate pairs
 */
public class UnicodeEscapeParserStateMachine {

    // Buffer for collecting hex digits
    private final StringBuilder hexBuffer = new StringBuilder(4);
    private final Utf16Sink sink;
    private char highSurrogate;
    // Surrogate pair tracking
    private boolean waitingForLowSurrogate = false;

    /**
     * Creates a new parser that writes to the given sink.
     *
     * @param sink the sink to write UTF-16 chars to
     */
    public UnicodeEscapeParserStateMachine(Utf16Sink sink) {
        this.sink = sink;
    }

    // Position tracking for error messages
//    private int position = 0;

    /**
     * Parses a string that may contain Unicode escape sequences and writes the
     * resulting UTF-16 chars to the sink.
     *
     * @param input the string to parse
     * @throws IllegalArgumentException if the input contains invalid Unicode sequences
     */
    public void parse(String input) {
        // Reset parser state
        State state = State.NORMAL;
        waitingForLowSurrogate = false;

        for (int i = 0; i < input.length(); i++) {
            state = processChar(state, input.charAt(i), i);
        }

        // Check for incomplete sequences at the end
        finalize2(state, input.length());
    }

    /**
     * Get a description of a character for error messages.
     *
     * @param c the character to describe
     * @return a human-readable description of the character
     */
    private String charDescription(char c) {
        if (c < 32 || c > 126) {
            return String.format("U+%04X", (int) c);
        } else {
            return "'" + c + "'";
        }
    }

    /**
     * Finalize the parsing, handling any incomplete sequences.
     *
     * @throws IllegalArgumentException if there's an incomplete sequence at the end
     */
    private void finalize2(State state, int position) {
        switch (state) {
            case BACKSLASH_SEEN:
                throw new IllegalArgumentException(
                        "Incomplete escape sequence at the end of input");

            case UNICODE_START:
                throw new IllegalArgumentException(
                        "Incomplete Unicode escape sequence at the end of input");

            case UNICODE_DIGITS:
                // Process any remaining hex digits
                if (hexBuffer.length() < 2) {
                    throw new IllegalArgumentException(
                            "Unicode escape needs at least 2 hex digits at the end of input");
                }

                try {
                    int codePoint = Integer.parseInt(hexBuffer.toString(), 16);
                    processUnicodeChar((char) codePoint, position);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            "Invalid hex digits in Unicode escape at the end of input", e);
                }
                break;
        }

        // Check for dangling high surrogate
        if (waitingForLowSurrogate) {
            throw new IllegalArgumentException(
                    "Dangling high surrogate at the end of input");
        }
    }

    /**
     * Checks if a character is a valid hexadecimal digit.
     *
     * @param c the character to check
     * @return true if the character is a hex digit (0-9, a-f, A-F)
     */
    private boolean isHexDigit(char c) {
        return (c >= '0' && c <= '9') ||
                (c >= 'a' && c <= 'f') ||
                (c >= 'A' && c <= 'F');
    }

    /**
     * Process a single character according to the current state.
     *
     * @param c the character to process
     * @throws IllegalArgumentException if the character creates an invalid sequence
     */
    private State processChar(State state, char c, int position) {
        do {
            switch (state) {
                case NORMAL:
                    if (c == '\\') {
                        state = State.BACKSLASH_SEEN;
                    } else {
                        processRegularChar(c, position);
                    }
                    return state;

                case BACKSLASH_SEEN:
                    if (c == '\\') {
                        // Escaped backslash
                        processRegularChar('\\', position);
                        state = State.NORMAL;
                    } else if (c == 'u') {
                        // Start of Unicode escape
                        hexBuffer.setLength(0);
                        state = State.UNICODE_START;
                    } else {
                        // Backslash followed by something else - treat as literal chars
                        processRegularChar('\\', position);
                        processRegularChar(c, position);
                        state = State.NORMAL;
                    }
                    return state;

                case UNICODE_START:
                    if (isHexDigit(c)) {
                        // First hex digit
                        hexBuffer.append(c);
                        state = State.UNICODE_DIGITS;
                        return state;
                    }
                    throw new IllegalArgumentException(
                            "Expected hex digit after \\u at position " + position);

                case UNICODE_DIGITS:
                    if (isHexDigit(c) && hexBuffer.length() < 4) {
                        // Collect up to 4 hex digits
                        hexBuffer.append(c);
                        return state;
                    }
                    // Either we have 4 digits or encountered a non-hex character
                    if (hexBuffer.length() < 2) {
                        throw new IllegalArgumentException(
                                "Unicode escape needs at least 2 hex digits at position " + position);
                    }

                    // Process the collected hex digits
                    try {
                        int codePoint = Integer.parseInt(hexBuffer.toString(), 16);
                        processUnicodeChar((char) codePoint, position);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(
                                "Invalid hex digits in Unicode escape at position " + position, e);
                    }

                    // Go back to process the current character again in NORMAL state
                    state = State.NORMAL;
                    break;
            }
        } while (true);
    }

    /**
     * Process a regular (non-escape-sequence) character.
     *
     * @param c the character to process
     * @throws IllegalArgumentException if the character creates an invalid surrogate sequence
     */
    private void processRegularChar(char c, int position) {
        if (waitingForLowSurrogate) {
            if (Character.isLowSurrogate(c)) {
                // Complete surrogate pair
                int codePoint = Character.toCodePoint(highSurrogate, c);
                waitingForLowSurrogate = false;

                // Output the combined character
                if (codePoint >= 0x10000) {
                    // Write the surrogate pair components
                    sink.accept(highSurrogate);
                    sink.accept(c);
                } else {
                    // Shouldn't happen with valid surrogate pairs, but just in case
                    sink.accept((char) codePoint);
                }
            } else {
                throw new IllegalArgumentException(
                        "Expected low surrogate but got " + charDescription(c) + " at position " + position);
            }
        } else if (Character.isHighSurrogate(c)) {
            // Start of surrogate pair
            highSurrogate = c;
            waitingForLowSurrogate = true;
        } else if (Character.isLowSurrogate(c)) {
            throw new IllegalArgumentException(
                    "Unexpected low surrogate without preceding high surrogate at position " + position);
        } else {
            // Regular character
            sink.accept(c);
        }
    }

    /**
     * Process a character decoded from a Unicode escape sequence.
     *
     * @param c the character decoded from the escape sequence
     * @throws IllegalArgumentException if the character creates an invalid surrogate sequence
     */
    private void processUnicodeChar(char c, int position) {
        if (waitingForLowSurrogate) {
            if (Character.isLowSurrogate(c)) {
                // Complete surrogate pair
                waitingForLowSurrogate = false;

                // Output the combined character
                sink.accept(highSurrogate);
                sink.accept(c);
            } else {
                throw new IllegalArgumentException(
                        "Expected low surrogate but got " + charDescription(c) +
                                " from Unicode escape at position " + position);
            }
        } else if (Character.isHighSurrogate(c)) {
            // Start of surrogate pair
            highSurrogate = c;
            waitingForLowSurrogate = true;
        } else if (Character.isLowSurrogate(c)) {
            throw new IllegalArgumentException(
                    "Unexpected low surrogate from Unicode escape without preceding high surrogate at position " + position);
        } else {
            // Regular character
            sink.accept(c);
        }
    }

    // Parser states
    private enum State {
        NORMAL,             // Regular text processing
        BACKSLASH_SEEN,     // Saw a '\', might be an escape sequence
        UNICODE_START,      // Saw '\\u', start of Unicode escape
        UNICODE_DIGITS,     // Processing hex digits of Unicode escape
    }

    /**
     * Interface for a sink that accepts UTF-16 chars.
     */
    public interface Utf16Sink {
        void accept(char c);
    }
}

