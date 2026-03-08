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

package io.questdb.std.fastdouble;

import io.questdb.std.NumericException;

/**
 * Parses a {@code FloatingPointLiteral} from a {@link CharSequence}.
 * <p>
 * This class should have a type parameter for the return value of its parse
 * methods. Unfortunately Java does not support type parameters for primitive
 * types. As a workaround we use {@code long}. A {@code long} has enough bits to
 * fit a {@code double} value or a {@code float} value.
 * <p>
 * See {@link io.questdb.std.fastdouble} for the grammar of
 * {@code FloatingPointLiteral}.
 */
final class FastFloat {

    private static float fallbackToJavaParser(
            CharSequence str,
            int startIndex,
            int endIndex,
            boolean rejectOverflow
    ) throws NumericException {
        final float f = Float.parseFloat(str.subSequence(startIndex, endIndex).toString());
        if (rejectOverflow &&
                (
                        f == Float.POSITIVE_INFINITY
                                || f == Float.NEGATIVE_INFINITY
                                || f == 0.0f
                )
        ) {
            throw NumericException.INSTANCE;
        }
        return f;
    }

    private static boolean isDigit(char c) {
        return '0' <= c && c <= '9';
    }

    /**
     * Parses a {@code DecimalFloatingPointLiteral} production with optional
     * trailing white space until the end of the text.
     * Given that we have already consumed the optional leading zero of
     * the {@code DecSignificand}.
     * <blockquote>
     * <dl>
     * <dt><i>DecimalFloatingPointLiteralWithWhiteSpace:</i></dt>
     * <dd><i>DecimalFloatingPointLiteral [WhiteSpace] EOT</i></dd>
     * </dl>
     * </blockquote>
     * See {@link io.questdb.std.fastdouble} for the grammar of
     * {@code DecimalFloatingPointLiteral} and {@code DecSignificand}.
     *
     * @param str            a string
     * @param index          start index inclusive of the {@code DecimalFloatingPointLiteralWithWhiteSpace}
     * @param endIndex       end index (exclusive)
     * @param isNegative     true if the float value is negative
     * @param hasLeadingZero true if we have consumed the optional leading zero
     * @return the bit pattern of the parsed value, if the input is legal;
     * otherwise, {@code -1L}.
     */
    private static float parseDecFloatLiteral(
            final CharSequence str,
            int index,
            int startIndex,
            int endIndex,
            boolean isNegative,
            boolean hasLeadingZero,
            boolean rejectOverflow
    ) throws NumericException {
        // Parse significand
        // -----------------
        // Note: a multiplication by a constant is cheaper than an
        //       arbitrary integer multiplication.
        long significand = 0;// significand is treated as an unsigned long
        final int significandStartIndex = index;
        int virtualIndexOfPoint = -1;
        boolean illegal = false;
        char ch = 0;
        for (; index < endIndex; index++) {
            ch = str.charAt(index);
            if (isDigit(ch)) {
                // This might overflow, we deal with it later.
                significand = 10 * significand + ch - '0';
            } else if (ch == '.') {
                illegal |= virtualIndexOfPoint >= 0;
                virtualIndexOfPoint = index;
                for (; index < endIndex - 8; index += 8) {
                    int eightDigits = tryToParseEightDigits(str, index + 1);
                    if (eightDigits < 0) {
                        break;
                    }
                    // This might overflow, we deal with it later.
                    significand = 100_000_000L * significand + eightDigits;
                }
            } else {
                break;
            }
        }
        final int digitCount;
        final int significandEndIndex = index;
        int exponent;
        if (virtualIndexOfPoint < 0) {
            digitCount = significandEndIndex - significandStartIndex;
            virtualIndexOfPoint = significandEndIndex;
            exponent = 0;
        } else {
            digitCount = significandEndIndex - significandStartIndex - 1;
            exponent = virtualIndexOfPoint - significandEndIndex + 1;
        }

        // Parse exponent number
        // ---------------------
        int expNumber = 0;
        if (ch == 'e' || ch == 'E') {
            ch = ++index < endIndex ? str.charAt(index) : 0;
            boolean neg_exp = ch == '-';
            if (neg_exp || ch == '+') {
                ch = ++index < endIndex ? str.charAt(index) : 0;
            }
            illegal |= !isDigit(ch);
            do {
                // Guard against overflow
                if (expNumber < FastDoubleUtils.MAX_EXPONENT_NUMBER) {
                    expNumber = 10 * expNumber + ch - '0';
                }
                ch = ++index < endIndex ? str.charAt(index) : 0;
            } while (isDigit(ch));
            if (neg_exp) {
                expNumber = -expNumber;
            }
            exponent += expNumber;
        }

        // Skip optional FloatTypeSuffix
        // ------------------------
        if (index < endIndex && (ch == 'f' || ch == 'F')) {
            index++;
        }

        // Skip trailing whitespace and check if FloatingPointLiteral is complete
        // ------------------------
        index = skipWhitespace(str, index, endIndex);
        if (illegal || index < endIndex
                || !hasLeadingZero && digitCount == 0) {
            throw NumericException.INSTANCE;
        }

        // Re-parse significand in case of a potential overflow
        // -----------------------------------------------
        final boolean isSignificandTruncated;
        int skipCountInTruncatedDigits = 0;//counts +1 if we skipped over the decimal point
        int exponentOfTruncatedSignificand;
        if (digitCount > 19) {
            significand = 0;
            for (index = significandStartIndex; index < significandEndIndex; index++) {
                ch = str.charAt(index);
                if (ch == '.') {
                    skipCountInTruncatedDigits++;
                } else {
                    if (Long.compareUnsigned(significand, FastDoubleUtils.MINIMAL_NINETEEN_DIGIT_INTEGER) < 0) {
                        significand = 10 * significand + ch - '0';
                    } else {
                        break;
                    }
                }
            }
            isSignificandTruncated = (index < significandEndIndex);
            exponentOfTruncatedSignificand = virtualIndexOfPoint - index + skipCountInTruncatedDigits + expNumber;
        } else {
            isSignificandTruncated = false;
            exponentOfTruncatedSignificand = 0;
        }
        return valueOfFloatLiteral(
                str,
                startIndex,
                endIndex,
                isNegative,
                significand,
                exponent,
                isSignificandTruncated,
                exponentOfTruncatedSignificand,
                rejectOverflow
        );
    }

    /**
     * Parses the following rules
     * (more rules are defined in {@link FastDoubleUtils}):
     * <dl>
     * <dt><i>RestOfHexFloatingPointLiteral</i>:
     * <dd><i>RestOfHexSignificand BinaryExponent</i>
     * </dl>
     *
     * <dl>
     * <dt><i>RestOfHexSignificand:</i>
     * <dd><i>HexDigits</i>
     * <dd><i>HexDigits</i> {@code .}
     * <dd><i>[HexDigits]</i> {@code .} <i>HexDigits</i>
     * </dl>
     *
     * @param str            the input string
     * @param index          index to the first character of RestOfHexFloatingPointLiteral
     * @param startIndex     the start index of the string
     * @param endIndex       the end index of the string
     * @param isNegative     if the resulting number is negative
     * @param rejectOverflow reject parsed values that overflow double type
     * @return the parsed value, if the input is legal;
     * @throws NumericException when input is illegal
     */
    private static float parseHexFloatLiteral(
            CharSequence str,
            int index,
            int startIndex,
            int endIndex,
            boolean isNegative,
            boolean rejectOverflow
    ) throws NumericException {

        // Parse HexSignificand
        // ------------
        long significand = 0;// significand is treated as an unsigned long
        int exponent = 0;
        final int significandStartIndex = index;
        int virtualIndexOfPoint = -1;
        final int digitCount;
        boolean illegal = false;
        char ch = 0;
        for (; index < endIndex; index++) {
            ch = str.charAt(index);
            // Table look up is faster than a sequence of if-else-branches.
            int hexValue = ch > 127 ? FastDoubleUtils.OTHER_CLASS : FastDoubleUtils.CHAR_TO_HEX_MAP[ch];
            if (hexValue >= 0) {
                significand = (significand << 4) | hexValue;// This might overflow, we deal with it later.
            } else if (hexValue == FastDoubleUtils.DECIMAL_POINT_CLASS) {
                illegal |= virtualIndexOfPoint >= 0;
                virtualIndexOfPoint = index;
            } else {
                break;
            }
        }
        final int significandEndIndex = index;
        if (virtualIndexOfPoint < 0) {
            digitCount = significandEndIndex - significandStartIndex;
            virtualIndexOfPoint = significandEndIndex;
        } else {
            digitCount = significandEndIndex - significandStartIndex - 1;
            exponent = Math.min(virtualIndexOfPoint - index + 1, FastDoubleUtils.MAX_EXPONENT_NUMBER) * 4;
        }

        // Parse exponent
        // --------------
        int expNumber = 0;
        final boolean hasExponent = (ch == 'p') || (ch == 'P');
        if (hasExponent) {
            ch = ++index < endIndex ? str.charAt(index) : 0;
            boolean neg_exp = ch == '-';
            if (neg_exp || ch == '+') {
                ch = ++index < endIndex ? str.charAt(index) : 0;
            }
            illegal |= !isDigit(ch);
            do {
                expNumber = 10 * expNumber + ch - '0';
                ch = ++index < endIndex ? str.charAt(index) : 0;
            } while (isDigit(ch));
            if (neg_exp) {
                expNumber = -expNumber;
            }
            exponent += expNumber;
        }

        // Skip optional FloatTypeSuffix
        // ------------------------
        if (index < endIndex && (ch == 'd' || ch == 'D' || ch == 'f' || ch == 'F')) {
            index++;
        }

        // Skip trailing whitespace and check if FloatingPointLiteral is complete
        // ------------------------
        index = skipWhitespace(str, index, endIndex);
        if (illegal || index < endIndex
                || digitCount == 0
                || !hasExponent) {
            throw NumericException.INSTANCE;
        }

        // Re-parse significand in case of a potential overflow
        // -----------------------------------------------
        final boolean isSignificandTruncated;
        int skipCountInTruncatedDigits = 0;//counts +1 if we skipped over the decimal point
        if (digitCount > 16) {
            significand = 0;
            for (index = significandStartIndex; index < significandEndIndex; index++) {
                ch = str.charAt(index);
                // Table look up is faster than a sequence of if-else-branches.
                int hexValue = ch > 127 ? FastDoubleUtils.OTHER_CLASS : FastDoubleUtils.CHAR_TO_HEX_MAP[ch];
                if (hexValue >= 0) {
                    if (Long.compareUnsigned(significand, FastDoubleUtils.MINIMAL_NINETEEN_DIGIT_INTEGER) < 0) {
                        significand = (significand << 4) | hexValue;
                    } else {
                        break;
                    }
                } else {
                    skipCountInTruncatedDigits++;
                }
            }
            isSignificandTruncated = (index < significandEndIndex);
        } else {
            isSignificandTruncated = false;
        }

        return valueOfHexLiteral(
                str,
                startIndex,
                endIndex,
                isNegative,
                significand,
                exponent,
                isSignificandTruncated,
                virtualIndexOfPoint - index + skipCountInTruncatedDigits + expNumber,
                rejectOverflow
        );
    }

    /**
     * Parses a {@code Infinity} production with optional trailing white space
     * until the end of the text.
     * <blockquote>
     * <dl>
     * <dt><i>InfinityWithWhiteSpace:</i></dt>
     * <dd>{@code Infinity} <i>[WhiteSpace] EOT</i></dd>
     * </dl>
     * </blockquote>
     *
     * @param str      a string
     * @param index    index of the "I" character
     * @param endIndex end index (exclusive)
     * @return a positive or negative infinity value
     * @throws NumberFormatException on parsing failure
     */
    private static float parseInfinity(CharSequence str, int index, int endIndex, boolean negative) throws NumericException {
        if (index + 7 < endIndex
                && str.charAt(index) == 'I'
                && str.charAt(index + 1) == 'n'
                && str.charAt(index + 2) == 'f'
                && str.charAt(index + 3) == 'i'
                && str.charAt(index + 4) == 'n'
                && str.charAt(index + 5) == 'i'
                && str.charAt(index + 6) == 't'
                && str.charAt(index + 7) == 'y'
        ) {
            index = skipWhitespace(str, index + 8, endIndex);
            if (index == endIndex) {
                return negative ? negativeInfinity() : positiveInfinity();
            }
        }
        throw NumericException.INSTANCE;
    }

    /**
     * Parses a {@code Nan} production with optional trailing white space
     * until the end of the text.
     * Given that the String contains a 'N' character at the current
     * {@code index}.
     * <blockquote>
     * <dl>
     * <dt><i>NanWithWhiteSpace:</i></dt>
     * <dd>{@code NaN} <i>[WhiteSpace] EOT</i></dd>
     * </dl>
     * </blockquote>
     *
     * @param str      a string that contains a "N" character at {@code index}
     * @param index    index of the "N" character
     * @param endIndex end index (exclusive)
     * @return a NaN value
     * @throws NumberFormatException on parsing failure
     */
    private static float parseNaN(CharSequence str, int index, int endIndex) throws NumericException {
        if (index + 2 < endIndex
                // && str.charAt(index) == 'N'
                && str.charAt(index + 1) == 'a'
                && str.charAt(index + 2) == 'N') {

            index = skipWhitespace(str, index + 3, endIndex);
            if (index == endIndex) {
                return nan();
            }
        }
        throw NumericException.INSTANCE;
    }

    /**
     * Skips optional white space in the provided string
     *
     * @param str      a string
     * @param index    start index (inclusive) of the optional white space
     * @param endIndex end index (exclusive) of the optional white space
     * @return index after the optional white space
     */
    private static int skipWhitespace(CharSequence str, int index, int endIndex) {
        for (; index < endIndex; index++) {
            if (str.charAt(index) > ' ') {
                break;
            }
        }
        return index;
    }

    private static int tryToParseEightDigits(CharSequence str, int offset) {
        // Performance: We extract the chars in two steps so that we
        //              can benefit from out of order execution in the CPU.
        long first = str.charAt(offset)
                | (long) str.charAt(offset + 1) << 16
                | (long) str.charAt(offset + 2) << 32
                | (long) str.charAt(offset + 3) << 48;

        long second = str.charAt(offset + 4)
                | (long) str.charAt(offset + 5) << 16
                | (long) str.charAt(offset + 6) << 32
                | (long) str.charAt(offset + 7) << 48;

        return FastDoubleSwar.tryToParseEightDigitsUtf16(first, second);
    }

    static float nan() {
        return Float.NaN;
    }

    static float negativeInfinity() {
        return Float.NEGATIVE_INFINITY;
    }

    /**
     * Parses a {@code FloatingPointLiteral} production with optional leading and trailing
     * white space.
     * <blockquote>
     * <dl>
     * <dt><i>FloatingPointLiteralWithWhiteSpace:</i></dt>
     * <dd><i>[WhiteSpace] FloatingPointLiteral [WhiteSpace]</i></dd>
     * </dl>
     * </blockquote>
     * See {@link io.questdb.std.fastdouble} for the grammar of
     * {@code FloatingPointLiteral}.
     *
     * @param str    a string containing a {@code FloatingPointLiteralWithWhiteSpace}
     * @param offset start offset of {@code FloatingPointLiteralWithWhiteSpace} in {@code str}
     * @param length length of {@code FloatingPointLiteralWithWhiteSpace} in {@code str}
     * @return the bit pattern of the parsed value, if the input is legal;
     * otherwise, {@code -1L}.
     */
    static float parseFloatingPointLiteral(CharSequence str, int offset, int length, boolean rejectOverflow) throws NumericException {
        final int endIndex = offset + length;
        if (offset < 0 || endIndex > str.length()) {
            throw NumericException.INSTANCE;
        }

        // Skip leading whitespace
        // -------------------
        int index = skipWhitespace(str, offset, endIndex);
        if (index == endIndex) {
            throw NumericException.INSTANCE;
        }
        char ch = str.charAt(index);

        // Parse optional sign
        // -------------------
        final boolean isNegative = ch == '-';
        if (isNegative || ch == '+') {
            ch = ++index < endIndex ? str.charAt(index) : 0;
            if (ch == 0) {
                throw NumericException.INSTANCE;
            }
        }

        // Parse NaN or Infinity
        // ---------------------
        if (ch >= 'I') {
            return ch == 'N'
                    ? parseNaN(str, index, endIndex)
                    : parseInfinity(str, index, endIndex, isNegative);
        }

        // Parse optional leading zero
        // ---------------------------
        final boolean hasLeadingZero = ch == '0';
        if (hasLeadingZero) {
            ch = ++index < endIndex ? str.charAt(index) : 0;
            if (ch == 'x' || ch == 'X') {
                return parseHexFloatLiteral(str, index + 1, offset, endIndex, isNegative, rejectOverflow);
            }
        }

        return parseDecFloatLiteral(str, index, offset, endIndex, isNegative, hasLeadingZero, rejectOverflow);
    }

    static float positiveInfinity() {
        return Float.POSITIVE_INFINITY;
    }

    static float valueOfFloatLiteral(
            CharSequence str,
            int startIndex,
            int endIndex,
            boolean isNegative,
            long significand,
            int exponent,
            boolean isSignificandTruncated,
            int exponentOfTruncatedSignificand,
            boolean rejectOverflow
    ) throws NumericException {
        float f = FastFloatMath.decFloatLiteralToFloat(
                isNegative,
                significand,
                exponent,
                isSignificandTruncated,
                exponentOfTruncatedSignificand
        );

        if (Float.isNaN(f)) {
            f = fallbackToJavaParser(str, startIndex, endIndex, rejectOverflow);
        }
        return f;
    }

    static float valueOfHexLiteral(
            CharSequence str,
            int startIndex,
            int endIndex,
            boolean isNegative,
            long significand,
            int exponent,
            boolean isSignificandTruncated,
            int exponentOfTruncatedSignificand,
            boolean rejectOverflow
    ) throws NumericException {
        float f = FastFloatMath.hexFloatLiteralToFloat(
                isNegative,
                significand,
                exponent,
                isSignificandTruncated,
                exponentOfTruncatedSignificand
        );

        if (Float.isNaN(f)) {
            f = fallbackToJavaParser(str, startIndex, endIndex, rejectOverflow);
        }
        return f;
    }
}
