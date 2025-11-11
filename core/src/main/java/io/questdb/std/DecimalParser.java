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

package io.questdb.std;

/**
 * Parser for converting string into decimal types.
 * <p>
 * This parser supports:
 * <ul>
 * <li>Standard decimal notation: "123.45", "-0.001"</li>
 * <li>Scientific notation: "1.23e5", "4.56e-3"</li>
 * <li>Special values: "NaN", "Infinity" (converted to NULL)</li>
 * <li>QuestDB decimal suffix: "123.45m" or "123.45M"</li>
 * <li>Floating-point suffixes: "123.45f", "123.45d" (stripped during parsing)</li>
 * <li>Leading zeros: "00123.45" → 123.45</li>
 * <li>Leading/trailing whitespace</li>
 * </ul>
 *
 * @see Decimal
 * @see Decimal64
 * @see Decimal128
 * @see Decimal256
 */
public final class DecimalParser {
    private DecimalParser() {
    }

    /**
     * Parses a decimal number from a CharSequence and stores the result in the provided Decimal instance.
     * <p>
     * This is the main parsing method that handles all decimal formats including scientific notation,
     * special values (NaN/Infinity), and various suffixes. The parser validates precision and scale
     * constraints and can optionally perform lossy truncation.
     * <p>
     * <b>Parsing Process:</b>
     * <ol>
     * <li>Strip suffixes (m, M, f, d) and leading/trailing whitespace</li>
     * <li>Parse sign (+/-) and detect special values (NaN/Infinity)</li>
     * <li>Remove leading zeros</li>
     * <li>Extract mantissa and locate decimal point</li>
     * <li>Parse exponent (if present)</li>
     * <li>Validate precision and scale constraints</li>
     * <li>Build the decimal value digit by digit using {@link Decimal#addPowerOfTenMultiple}</li>
     * </ol>
     * <p>
     * <b>Examples:</b>
     * <pre>
     * // Basic parsing
     * parse(decimal, "123.45", 0, 6, -1, -1, false, false)
     * // Result: precision=5, scale=2, value=123.45
     *
     * // With precision limit
     * parse(decimal, "123.456", 0, 7, 5, -1, false, false)
     * // Throws: NumericException (requires precision 6, limited to 5)
     *
     * // With scale limit (strict)
     * parse(decimal, "123.456", 0, 7, -1, 2, true, false)
     * // Throws: NumericException (has 3 decimal places, limited to 2)
     *
     * // With scale limit (lossy)
     * parse(decimal, "123.456", 0, 7, -1, 2, false, true)
     * // Result: precision=5, scale=2, value=123.45 (truncated)
     *
     * // Scientific notation
     * parse(decimal, "1.23e5", 0, 6, -1, -1, false, false)
     * // Result: precision=6, scale=0, value=123000
     *
     * // Negative exponent
     * parse(decimal, "123e-2", 0, 6, -1, -1, false, false)
     * // Result: precision=4, scale=2, value=1.23
     * </pre>
     *
     * @param decimal   the Decimal instance to store the parsed result (modified in-place)
     * @param cs        the CharSequence containing the decimal number to parse
     * @param lo        the starting index (inclusive) in the CharSequence
     * @param hi        the ending index (exclusive) in the CharSequence
     * @param precision the maximum allowed precision, or -1 for no limit. If the parsed value
     *                  requires more precision, a NumericException is thrown
     * @param scale     the target scale (decimal places), or -1 to use the natural scale from
     *                  the input. If the input has more decimal places than this and lossy=false,
     *                  a NumericException is thrown
     * @param strict    if true, trailing zeros are preserved in scale calculation; if false,
     *                  trailing zeros after the decimal point are removed (e.g., "1.200" → scale 1)
     * @param lossy     if true and scale is specified, extra decimal places are silently truncated;
     *                  if false, extra decimal places cause a NumericException
     * @return a long containing both the actual precision (low 32 bits) and scale (high 32 bits)
     * of the parsed value. Use {@link Numbers#decodeLowInt} to extract precision and
     * {@link Numbers#decodeHighInt} to extract scale
     * @throws NumericException if the input is not a valid decimal number, or if precision/scale
     *                          constraints are violated, or if the value exceeds the decimal type's capacity
     */
    public static long parse(Decimal decimal, CharSequence cs, int lo, int hi, int precision, int scale, boolean strict, boolean lossy) throws NumericException {
        int ch = hi > lo ? cs.charAt(hi - 1) | 32 : 0;
        // We don't want to parse the m suffix, we can safely skip it
        if (ch == 'm') {
            strict = true;
            hi--;
            ch = hi > lo ? cs.charAt(hi - 1) | 32 : 0;
        }

        // We also need to skip 'd' and 'f' when parsing doubles/floats
        if (ch == 'f' || ch == 'd') {
            hi--;
        }

        // Skip leading whitespaces
        while (lo < hi && cs.charAt(lo) == ' ') {
            lo++;
        }

        if (lo == hi) {
            throw NumericException.instance().put("invalid decimal: empty value");
        }

        // Parses sign
        boolean negative = false;
        if (cs.charAt(lo) == '-') {
            negative = true;
            lo++;
        } else if (cs.charAt(lo) == '+') {
            lo++;
        }

        if (lo == hi) {
            throw NumericException.instance().put("invalid decimal: empty value");
        }

        ch = cs.charAt(lo);
        if (ch >= 'I') {
            if (isNanOrInfinite((char) ch, cs, lo, hi)) {
                decimal.ofNull();
                return 0;
            }
        }

        // Remove leading zeros
        boolean skippedZeroes = false;
        while (lo < hi - 1 && cs.charAt(lo) == '0') {
            lo++;
            skippedZeroes = true;
        }

        // We do a first pass over the literal to ensure that the format is correct (numerical and at most 1 dot) and to
        // measure the given precision/scale.
        int dot = -1;
        boolean digitFound = false;
        int digitLo = lo;
        for (; lo < hi; lo++) {
            char c = cs.charAt(lo);
            if (isDigit(c)) {
                digitFound = true;
                continue;
            } else if (c == '.' && dot == -1) {
                dot = lo;
                continue;
            }
            break;
        }
        if (!digitFound) {
            if (skippedZeroes) {
                digitLo--;
            } else {
                throw NumericException.instance()
                        .put("invalid decimal: '").put(cs)
                        .put("' contains no digits");
            }
        }
        int digitHi = lo;
        if (!strict && dot != -1) {
            while (digitHi > dot && cs.charAt(digitHi - 1) == '0') {
                digitHi--;
            }
        }

        // Compute the scale of the given literal (e.g. '1.234' -> 3) and the total number of digits
        int literalScale = dot == -1 ? 0 : (digitHi - dot - 1);
        int literalDigits = digitHi - digitLo - (dot == -1 ? 0 : 1);

        int virtualScale = literalScale;

        int exp = 0;
        if (lo != hi) {
            // Parses exponent
            if ((cs.charAt(lo) | 32) == 'e') {
                exp = Numbers.parseInt(cs, lo + 1, hi);
                // Depending on whether exp is positive or negative, we have to virtually move the dot to the right
                // or to the left respectively
                if (exp >= 0 && exp > literalScale) {
                    exp -= literalScale;
                    virtualScale = 0;
                } else {
                    virtualScale -= exp;
                    exp = 0;
                }
            } else {
                throw NumericException.instance()
                        .put("decimal '").put(cs)
                        .put("' contains invalid character '")
                        .put(cs.charAt(digitHi)).put('\'');
            }
        }

        // If lossy is enabled, we can strip digits after the provided scale (as long as it is provided)
        if (lossy && scale >= 0 && virtualScale > scale) {
            int drop = virtualScale - scale;
            while (drop > 0 && digitHi > digitLo) {
                digitHi--;
                if (digitHi == dot) {
                    continue;
                }
                drop--;
            }
            virtualScale = scale;
            int hasDot = (dot >= digitLo && dot < digitHi) ? 1 : 0;
            literalDigits = digitHi - digitLo - hasDot;
        }

        // We still need to adjust the exponent if the user specifies a target scale.
        if (scale == -1) {
            scale = virtualScale;
        } else if (virtualScale <= scale) {
            exp += scale - virtualScale;
        } else {
            throw NumericException.instance()
                    .put("decimal '").put(cs)
                    .put("' has ").put(virtualScale).put(" decimal places but scale is limited to ").put(scale);
        }
        if (scale > decimal.getMaxScale()) {
            throw NumericException.instance()
                    .put("decimal '").put(cs)
                    .put("' exceeds maximum allowed scale of ").put(decimal.getMaxScale());
        }

        // At this point, the exponent might add zeroes (but not remove them, we've handled this case right before).
        // Knowing that the scale is right and that we have the correct amount of digits, computing the final precision
        // is trivial.
        // Note that contrary to the scale, it's alright to have a precision that is different than the user provided
        // precision, as long as it's lower.

        // Compute the final precision of the decimal
        int pow = literalDigits + exp;
        final int finalPrecision = Math.max(pow, scale + 1);
        if (precision != -1 && finalPrecision > precision) {
            throw NumericException.instance()
                    .put("decimal '").put(cs)
                    .put("' requires precision of ").put(finalPrecision)
                    .put(" but is limited to ").put(precision);
        }
        if (finalPrecision > decimal.getMaxPrecision()) {
            throw NumericException.instance()
                    .put("decimal '").put(cs)
                    .put("' exceeds maximum allowed precision of ").put(decimal.getMaxPrecision());
        }

        decimal.ofZero();
        for (int p = digitLo; p < digitHi; p++) {
            if (p == dot) {
                continue;
            }

            decimal.addPowerOfTenMultiple(--pow, cs.charAt(p) - '0');
        }
        decimal.setScale(scale);

        if (negative) {
            decimal.negate();
        }

        return Numbers.encodeLowHighInts(finalPrecision, scale);
    }

    /**
     * Checks if a character is a decimal digit (0-9).
     *
     * @param c the character to check
     * @return true if c is '0' through '9', false otherwise
     */
    private static boolean isDigit(char c) {
        return '0' <= c && c <= '9';
    }

    /**
     * Detects special floating-point values "NaN" and "Infinity" at the start of a CharSequence.
     * <p>
     * The check is case-sensitive and requires exact matches:
     * <ul>
     * <li>"NaN" (exactly 3 characters)</li>
     * <li>"Infinity" (exactly 8 characters)</li>
     * </ul>
     *
     * @param ch the first character at position lo (optimization to avoid redundant charAt call)
     * @param cs the CharSequence being parsed
     * @param lo the starting index where the special value should begin
     * @param hi the ending index (exclusive) of the CharSequence
     * @return true if "NaN" or "Infinity" is found at position lo, false otherwise
     */
    private static boolean isNanOrInfinite(char ch, CharSequence cs, int lo, int hi) {
        return (ch == 'N' && lo + 2 < hi && cs.charAt(lo + 1) == 'a' && cs.charAt(lo + 2) == 'N') ||
                (ch == 'I' && lo + 7 < hi && cs.charAt(lo + 1) == 'n' && cs.charAt(lo + 2) == 'f'
                        && cs.charAt(lo + 3) == 'i' && cs.charAt(lo + 4) == 'n' && cs.charAt(lo + 5) == 'i'
                        && cs.charAt(lo + 6) == 't' && cs.charAt(lo + 7) == 'y');
    }
}
