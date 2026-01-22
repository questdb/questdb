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
 * Evaluates date variable expressions like "$today + 3bd", "$yesterday", "$now - 5d".
 * <p>
 * Supported variables:
 * <ul>
 *   <li>{@code $today} - start of current day (00:00:00)</li>
 *   <li>{@code $now} - current timestamp (with time)</li>
 *   <li>{@code $yesterday} - start of yesterday</li>
 *   <li>{@code $tomorrow} - start of tomorrow</li>
 * </ul>
 * <p>
 * Supported arithmetic:
 * <ul>
 *   <li>{@code + Nd} or {@code - Nd} - add/subtract N calendar days</li>
 *   <li>{@code + Nbd} or {@code - Nbd} - add/subtract N business days (skip Sat/Sun)</li>
 * </ul>
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
        // Find end of variable name (until space or end)
        int varEnd = lo + 1;
        while (varEnd < hi && expression.charAt(varEnd) != ' ') {
            varEnd++;
        }

        // Resolve base variable
        long baseTimestamp = resolveVariable(timestampDriver, expression, lo, varEnd, nowTimestamp, errorPos);

        // Check for arithmetic operator
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
            throw SqlException.$(errorPos, "Expected '+' or '-' operator");
        }

        // Parse the offset value
        int offsetStart = opPos + 1;
        while (offsetStart < hi && expression.charAt(offsetStart) == ' ') {
            offsetStart++;
        }

        if (offsetStart >= hi) {
            throw SqlException.$(errorPos, "Expected number after operator");
        }

        // Find end of number
        int numEnd = offsetStart;
        while (numEnd < hi && Character.isDigit(expression.charAt(numEnd))) {
            numEnd++;
        }

        if (numEnd == offsetStart) {
            throw SqlException.$(errorPos, "Expected number after operator");
        }

        int offsetValue;
        try {
            offsetValue = Numbers.parseInt(expression, offsetStart, numEnd);
        } catch (NumericException e) {
            throw SqlException.$(errorPos, "Invalid number in date expression");
        }

        if (op == '-') {
            offsetValue = -offsetValue;
        }

        // Parse unit (d or bd)
        int unitStart = numEnd;
        while (unitStart < hi && expression.charAt(unitStart) == ' ') {
            unitStart++;
        }

        if (unitStart >= hi) {
            throw SqlException.$(errorPos, "Expected unit 'd' or 'bd' after number");
        }

        // Check for 'bd' (business days) or 'd' (calendar days)
        int remaining = hi - unitStart;
        int unitLen;
        boolean isBusinessDays;

        if (remaining >= 2 && (expression.charAt(unitStart) | 32) == 'b' && (expression.charAt(unitStart + 1) | 32) == 'd') {
            // Business days
            unitLen = 2;
            isBusinessDays = true;
        } else if ((expression.charAt(unitStart) | 32) == 'd') {
            // Calendar days
            unitLen = 1;
            isBusinessDays = false;
        } else {
            throw SqlException.$(errorPos, "Invalid unit, expected 'd' or 'bd'");
        }

        // Check for unexpected trailing characters after unit (caller trims whitespace)
        if (unitStart + unitLen != hi) {
            throw SqlException.$(errorPos, "Unexpected characters after unit");
        }

        if (isBusinessDays) {
            return addBusinessDays(timestampDriver, baseTimestamp, offsetValue);
        } else {
            return timestampDriver.addDays(baseTimestamp, offsetValue);
        }
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

    private static long resolveVariable(
            TimestampDriver timestampDriver,
            CharSequence expression,
            int lo,
            int hi,
            long nowTimestamp,
            int errorPos
    ) throws SqlException {
        // Extract variable name (without $)
        int varStart = lo + 1; // skip '$'

        if (Chars.equalsIgnoreCase("today", expression, varStart, hi)) {
            return timestampDriver.startOfDay(nowTimestamp, 0);
        }
        if (Chars.equalsIgnoreCase("now", expression, varStart, hi)) {
            return nowTimestamp;
        }
        if (Chars.equalsIgnoreCase("yesterday", expression, varStart, hi)) {
            return timestampDriver.startOfDay(nowTimestamp, -1);
        }
        if (Chars.equalsIgnoreCase("tomorrow", expression, varStart, hi)) {
            return timestampDriver.startOfDay(nowTimestamp, 1);
        }

        throw SqlException.$(errorPos, "Unknown date variable: ").put(expression.subSequence(lo, hi));
    }
}
