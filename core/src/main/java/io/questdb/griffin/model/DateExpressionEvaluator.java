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

package io.questdb.griffin.model;

import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;

/**
 * Evaluates date variable expressions like "$today + 3bd", "$yesterday", "$now - 5h".
 * <p>
 * Supported variables:
 * <ul>
 *   <li>{@code $today} - start of current day (00:00:00)</li>
 *   <li>{@code $now} - current timestamp (with time)</li>
 *   <li>{@code $yesterday} - start of yesterday</li>
 *   <li>{@code $tomorrow} - start of tomorrow</li>
 * </ul>
 * <p>
 * Supported arithmetic units:
 * <ul>
 *   <li>{@code y} - years (calendar-aware)</li>
 *   <li>{@code M} - months (calendar-aware)</li>
 *   <li>{@code w} - weeks</li>
 *   <li>{@code d} - calendar days</li>
 *   <li>{@code bd} - business days (skip Sat/Sun)</li>
 *   <li>{@code h} - hours</li>
 *   <li>{@code m} - minutes</li>
 *   <li>{@code s} - seconds</li>
 *   <li>{@code T} - milliseconds</li>
 *   <li>{@code u} - microseconds</li>
 *   <li>{@code n} - nanoseconds</li>
 * </ul>
 * <p>
 * Examples: {@code $now - 1h}, {@code $today + 30m}, {@code $now - 500T}
 */
public class DateExpressionEvaluator {
    private static final int SATURDAY = 6;
    private static final int SUNDAY = 7;

    /**
     * Evaluates a date expression and returns the resolved timestamp.
     *
     * @param timestampDriver the timestamp driver for time calculations
     * @param expression      the expression to evaluate (e.g., "$today + 3bd")
     * @param lo              start index in expression
     * @param hi              end index in expression
     * @param nowTimestamp    the current timestamp for resolving variables
     * @param errorPos        position for error reporting
     * @return resolved timestamp in driver's units (micros or nanos)
     * @throws SqlException if the expression is invalid
     */
    public static long evaluate(
            TimestampDriver timestampDriver,
            CharSequence expression,
            int lo,
            int hi,
            long nowTimestamp,
            int errorPos
    ) throws SqlException {
        // Caller already trims whitespace and verifies first char is '$'
        // Find end of variable name (until space, '+', '-', or end)
        // This supports both "$today + 5d" (with spaces) and "$today+5d" (compact)
        int varEnd = lo + 1;
        for (; varEnd < hi; varEnd++) {
            char c = expression.charAt(varEnd);
            if (c == ' ' || c == '+' || c == '-') {
                break;
            }
        }

        // Resolve base variable
        long baseTimestamp = resolveVariable(timestampDriver, expression, lo, varEnd, nowTimestamp, errorPos);

        // Check for arithmetic operator (skip any spaces first)
        int opPos = varEnd;
        while (opPos < hi && expression.charAt(opPos) == ' ') {
            opPos++;
        }

        if (opPos >= hi) {
            // No arithmetic, just return the base value
            return baseTimestamp;
        }

        char op = expression.charAt(opPos);
        if (op != '+' && op != '-') {
            throw SqlException.$(errorPos + opPos - lo, "Expected '+' or '-' operator");
        }

        // Parse the offset value (skip any spaces after operator)
        int offsetStart = opPos + 1;
        while (offsetStart < hi && expression.charAt(offsetStart) == ' ') {
            offsetStart++;
        }

        if (offsetStart >= hi) {
            throw SqlException.$(errorPos + opPos - lo, "Expected number after operator");
        }

        // Find end of number (underscores are allowed as separators, e.g. 10_000)
        int numEnd = offsetStart;
        for (; numEnd < hi; numEnd++) {
            char c = expression.charAt(numEnd);
            if (c != '_' && !Chars.isAsciiDigit(c)) {
                break;
            }
        }

        if (numEnd == offsetStart) {
            throw SqlException.$(errorPos + offsetStart - lo, "Expected number after operator");
        }

        int offsetValue;
        try {
            offsetValue = Numbers.parseInt(expression, offsetStart, numEnd);
        } catch (NumericException e) {
            throw SqlException.$(errorPos + offsetStart - lo, "Invalid number in date expression");
        }

        if (op == '-') {
            offsetValue = -offsetValue;
        }

        // Parse unit
        int unitStart = numEnd;
        while (unitStart < hi && expression.charAt(unitStart) == ' ') {
            unitStart++;
        }

        if (unitStart >= hi) {
            throw SqlException.$(errorPos + numEnd - lo, "Expected time unit after number");
        }

        // Check for 'bd' (business days) first, then single-character units
        int remaining = hi - unitStart;
        char unitChar = expression.charAt(unitStart);

        // Check for 'bd' (business days) - case insensitive
        if (remaining >= 2 && (unitChar | 32) == 'b' && (expression.charAt(unitStart + 1) | 32) == 'd') {
            // Check for unexpected trailing characters
            if (unitStart + 2 != hi) {
                throw SqlException.$(errorPos + unitStart + 2 - lo, "Unexpected characters after unit");
            }
            return addBusinessDays(timestampDriver, baseTimestamp, offsetValue);
        }

        // Single-character units - check for trailing characters
        if (unitStart + 1 != hi) {
            throw SqlException.$(errorPos + unitStart + 1 - lo, "Unexpected characters after unit");
        }

        // Handle single-character units using timestampDriver.add()
        // Supported: y (years), M (months), w (weeks), d (days), h (hours),
        //            m (minutes), s (seconds), T (millis), u (micros), n (nanos)
        // Note: 'd' is case-insensitive for backward compatibility
        char normalizedUnit = (unitChar == 'D') ? 'd' : unitChar;
        long result = timestampDriver.add(baseTimestamp, normalizedUnit, offsetValue);
        if (result == Numbers.LONG_NULL) {
            throw SqlException.$(errorPos + unitStart - lo, "Invalid time unit: ").put(unitChar);
        }
        return result;
    }

    /**
     * Adds business days (skipping Saturday=6 and Sunday=7).
     */
    private static long addBusinessDays(TimestampDriver timestampDriver, long timestamp, int businessDays) {
        if (businessDays == 0) {
            return timestamp;
        }

        int direction = businessDays > 0 ? 1 : -1;
        int remaining = Math.abs(businessDays);
        long result = timestamp;

        while (remaining > 0) {
            result = timestampDriver.addDays(result, direction);
            int dow = timestampDriver.getDayOfWeek(result);
            if (dow != SATURDAY && dow != SUNDAY) {
                remaining--;
            }
        }
        return result;
    }

    /**
     * Checks whether the characters at seq[lo..lim) start with a known date
     * variable ({@code $now}, {@code $today}, {@code $yesterday}, {@code $tomorrow})
     * followed by a non-letter boundary. Case-insensitive, zero-allocation.
     */
    public static boolean isDateVariable(CharSequence seq, int lo, int lim) {
        if (lo >= lim || seq.charAt(lo) != '$') {
            return false;
        }
        int varStart = lo + 1;
        int remaining = lim - varStart;
        if (remaining < 3) {
            return false;
        }
        char c0 = (char) (seq.charAt(varStart) | 32);
        char c1 = (char) (seq.charAt(varStart + 1) | 32);
        char c2 = (char) (seq.charAt(varStart + 2) | 32);
        if (c0 == 'n' && c1 == 'o' && c2 == 'w') {
            return remaining == 3 || isVarBoundary(seq, varStart + 3, lim);
        }
        if (remaining >= 5) {
            char c3 = (char) (seq.charAt(varStart + 3) | 32);
            char c4 = (char) (seq.charAt(varStart + 4) | 32);
            if (c0 == 't' && c1 == 'o' && c2 == 'd' && c3 == 'a' && c4 == 'y') {
                return remaining == 5 || isVarBoundary(seq, varStart + 5, lim);
            }
            if (remaining >= 8) {
                char c5 = (char) (seq.charAt(varStart + 5) | 32);
                char c6 = (char) (seq.charAt(varStart + 6) | 32);
                char c7 = (char) (seq.charAt(varStart + 7) | 32);
                if (c0 == 't' && c1 == 'o' && c2 == 'm' && c3 == 'o' && c4 == 'r' && c5 == 'r' && c6 == 'o' && c7 == 'w') {
                    return remaining == 8 || isVarBoundary(seq, varStart + 8, lim);
                }
                if (remaining >= 9) {
                    char c8 = (char) (seq.charAt(varStart + 8) | 32);
                    if (c0 == 'y' && c1 == 'e' && c2 == 's' && c3 == 't' && c4 == 'e' && c5 == 'r' && c6 == 'd' && c7 == 'a' && c8 == 'y') {
                        return remaining == 9 || isVarBoundary(seq, varStart + 9, lim);
                    }
                }
            }
        }
        return false;
    }

    private static boolean isVarBoundary(CharSequence seq, int pos, int lim) {
        char c = seq.charAt(pos);
        if (Chars.isAsciiLetter(c)) {
            // 'T'/'t' followed by a digit is the ISO time-override suffix, not a name continuation
            return (c == 'T' || c == 't') && pos + 1 < lim && Chars.isAsciiDigit(seq.charAt(pos + 1));
        }
        return true;
    }

    private static long resolveVariable(
            TimestampDriver timestampDriver,
            CharSequence expression,
            int lo,
            int hi,
            long nowTimestamp,
            int errorPos
    ) throws SqlException {
        int varStart = lo + 1; // skip '$'
        switch (hi - varStart) {
            case 3:
                if ((expression.charAt(varStart) | 32) == 'n'
                        && (expression.charAt(varStart + 1) | 32) == 'o'
                        && (expression.charAt(varStart + 2) | 32) == 'w') {
                    return nowTimestamp;
                }
                break;
            case 5:
                if ((expression.charAt(varStart) | 32) == 't'
                        && (expression.charAt(varStart + 1) | 32) == 'o'
                        && (expression.charAt(varStart + 2) | 32) == 'd'
                        && (expression.charAt(varStart + 3) | 32) == 'a'
                        && (expression.charAt(varStart + 4) | 32) == 'y') {
                    return timestampDriver.startOfDay(nowTimestamp, 0);
                }
                break;
            case 8:
                if ((expression.charAt(varStart) | 32) == 't'
                        && (expression.charAt(varStart + 1) | 32) == 'o'
                        && (expression.charAt(varStart + 2) | 32) == 'm'
                        && (expression.charAt(varStart + 3) | 32) == 'o'
                        && (expression.charAt(varStart + 4) | 32) == 'r'
                        && (expression.charAt(varStart + 5) | 32) == 'r'
                        && (expression.charAt(varStart + 6) | 32) == 'o'
                        && (expression.charAt(varStart + 7) | 32) == 'w') {
                    return timestampDriver.startOfDay(nowTimestamp, 1);
                }
                break;
            case 9:
                if ((expression.charAt(varStart) | 32) == 'y'
                        && (expression.charAt(varStart + 1) | 32) == 'e'
                        && (expression.charAt(varStart + 2) | 32) == 's'
                        && (expression.charAt(varStart + 3) | 32) == 't'
                        && (expression.charAt(varStart + 4) | 32) == 'e'
                        && (expression.charAt(varStart + 5) | 32) == 'r'
                        && (expression.charAt(varStart + 6) | 32) == 'd'
                        && (expression.charAt(varStart + 7) | 32) == 'a'
                        && (expression.charAt(varStart + 8) | 32) == 'y') {
                    return timestampDriver.startOfDay(nowTimestamp, -1);
                }
                break;
        }
        throw SqlException.$(errorPos, "Unknown date variable: ").put(expression.subSequence(lo, hi));
    }
}
