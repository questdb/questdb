package io.questdb.client.impl;

import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;

/**
 * Zero allocation configuration string parser.
 * <p>
 * The usual way to use this class is to call {@link #of(CharSequence, StringSink)} to parse the schema.
 * <br>If the
 * method returns a non-negative value, then call {@link #nextKey(CharSequence, int, StringSink)} to parse the
 * key. If <code>nextKey()</code> returns a non-negative value, then call {@link #value(CharSequence, int, StringSink)} to
 * parse the value. Repeat until {@link #hasNext(CharSequence, int)} returns false.
 * <p>
 * Methods that return a position handler will return a negative value if they encounter an error. In this case,
 * the error message will be written to the output sink.
 * <p>
 * When the returned position handler is positive, it can be used to parse the next key or value.
 */
public final class ConfStringParser {
    private ConfStringParser() {
    }

    /**
     * Returns true if there is more to parse.
     * <p>
     *
     * @param input input
     * @param pos   position handler
     * @return true if there is more to parse
     */
    public static boolean hasNext(CharSequence input, int pos) {
        if (pos == -1) {
            return false;
        }
        return pos != input.length();
    }

    /**
     * Parses key from input and writes it to output.
     * <p>
     * Call this method only after a previous call to {@link #hasNext(CharSequence, int)} returned
     * true.
     * <p>
     * This method will return a negative value if it encounters an error. In this case, the error
     * message will be written to the output sink.
     *
     * @param input  input
     * @param pos    position handler
     * @param output output
     * @return next position handler, or negative value if error
     */
    public static int nextKey(CharSequence input, int pos, StringSink output) {
        output.clear();
        if (pos == -1) {
            return pos;
        }
        int n = input.length();
        int start = pos;
        for (; pos < n; pos++) {
            char c = input.charAt(pos);
            if (c == '=') {
                if (pos == start) {
                    output.put("empty key");
                    return -1;
                }
                Chars.toLowerCase(input, start, pos, output);
                return pos + 1;
            } else if (c == ';') {
                output.put("incomplete key-value pair before end of input at position ").put(pos);
                return -1;
            } else if (invalidIdentifierChar(c)) {
                output.put("key must be consist of alpha-numerical ascii characters and underscore, not '").put(c).put("' at position ").put(pos);
                return -1;
            }

        }
        output.clear();
        output.put("incomplete key-value pair before end of input at position ").put(pos);
        return -1;
    }

    /**
     * Parses schema name from input. Schema name must start with schema type, e.g. http::.
     * <p>
     * This is the starting point for all configuration parsing. It returns a position handler
     * that can be used to parse the rest of the configuration.
     * <p>
     * When parsing configuration, the parser will return -1 if it encounters an error. In this
     * case, the error message will be written to the output sink.
     *
     * @param input  input
     * @param output output
     * @return position handler, or negative value if error
     */
    public static int of(CharSequence input, StringSink output) {
        output.clear();
        char lastChar = 0;
        for (int i = 0, n = input.length(); i < n; i++) {
            char c = input.charAt(i);
            if (lastChar == ':') {
                if (c == ':') {
                    if (i == 1) {
                        output.put("schema is empty");
                        return -1;
                    }
                    if (i == n - 1) {
                        output.put("missing trailing semicolon at position ").put(i);
                        return -1;
                    }
                    Chars.toLowerCase(input, 0, i - 1, output);
                    return i + 1;
                } else {
                    output.put("bad separator, expected '::' got ':").put(c).put("' at position ").put(i - 1);
                    return -1;
                }
            } else if (c == ':') {
                lastChar = c;
            } else if (invalidIdentifierChar(c)) {
                output.put("bad separator, expected ':' got '").put(c).put("' at position ").put(i);
                return -1;
            } else {
                lastChar = c;
            }
        }
        output.put("schema name must start with schema type, e.g. http::");
        return -1;
    }

    /**
     * Parse value from input and write it to output.
     * <p>
     * Call this method only after a previous call to {@link #nextKey(CharSequence, int, StringSink)} returned
     * a non-negative value.
     * <p>
     * This method will return -1 if it encounters an error.
     *
     * @param input  input
     * @param pos    position handler
     * @param output output
     * @return next position handler, or negative value if error
     */
    public static int value(CharSequence input, int pos, StringSink output) {
        output.clear();
        if (pos == -1) {
            return -1;
        }
        for (int n = input.length(); pos < n; pos++) {
            char c = input.charAt(pos);
            if (c == ';') {
                if (++pos == n || input.charAt(pos) != ';') {
                    return pos;
                }
                output.put(';');
            } else {
                output.put(c);
            }
        }
        output.clear();
        output.put("missing trailing semicolon at position ").put(pos);
        return -1;
    }

    private static boolean invalidIdentifierChar(char c) {
        return !Character.isDigit(c) && c != '_' && !isAsciiLetter(c);
    }

    private static boolean isAsciiLetter(char c) {
        char lower = (char) (c | 0x20);
        return lower >= 'a' && lower <= 'z';
    }
}
