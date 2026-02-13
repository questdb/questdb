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
 * Encodes a single {@code $variable ± offset} expression as a value object.
 * The expression is pre-parsed at compile time so that evaluation at runtime
 * uses only pure long arithmetic (no string parsing).
 * <p>
 * Supported variables: {@code $now}, {@code $today}, {@code $yesterday}, {@code $tomorrow}.
 * <p>
 * Supported offset units: y, M, w, d, bd (business days), h, m, s, T (millis), u (micros), n (nanos).
 */
public class DateVariableExpr {
    public static final byte VAR_NOW = 0;
    public static final byte VAR_TODAY = 1;
    public static final byte VAR_TOMORROW = 2;
    public static final byte VAR_YESTERDAY = 3;

    private static final int SATURDAY = 6;
    private static final int SUNDAY = 7;

    private final boolean isBusinessDays;
    private final char offsetUnit; // 0 if no offset
    private final int offsetValue;
    private final byte varType;

    public DateVariableExpr(byte varType, char offsetUnit, int offsetValue, boolean isBusinessDays) {
        this.varType = varType;
        this.offsetUnit = offsetUnit;
        this.offsetValue = offsetValue;
        this.isBusinessDays = isBusinessDays;
    }

    /**
     * Evaluates this expression with the given "now" timestamp, producing
     * a resolved timestamp using only long arithmetic.
     *
     * @param driver the timestamp driver for time calculations
     * @param now    the current timestamp
     * @return resolved timestamp in the driver's units
     */
    public long evaluate(TimestampDriver driver, long now) {
        long base = switch (varType) {
            case VAR_NOW -> now;
            case VAR_TODAY -> driver.startOfDay(now, 0);
            case VAR_YESTERDAY -> driver.startOfDay(now, -1);
            case VAR_TOMORROW -> driver.startOfDay(now, 1);
            default -> throw new IllegalStateException("Unknown variable type: " + varType);
        };
        if (offsetUnit == 0 && !isBusinessDays) {
            return base;
        }
        if (isBusinessDays) {
            return addBusinessDays(driver, base, offsetValue);
        }
        char normalizedUnit = (offsetUnit == 'D') ? 'd' : offsetUnit;
        return driver.add(base, normalizedUnit, offsetValue);
    }

    public byte getVarType() {
        return varType;
    }

    /**
     * Parses a date variable expression directly into the compiled IR encoding.
     * No object allocation.
     * <p>
     * Layout: varType[59:58] | isBusinessDays[57] | offsetUnit[55:40] | offsetValue[31:0]
     *
     * @param seq      the expression string
     * @param lo       start index (must point to '$')
     * @param hi       end index (exclusive)
     * @param errorPos position for error reporting
     * @return the encoded expression as a long
     * @throws SqlException if the expression is invalid
     */
    public static long parseEncoded(CharSequence seq, int lo, int hi, int errorPos) throws SqlException {
        // Find end of variable name (until space, '+', '-', or end)
        int varEnd = lo + 1;
        for (; varEnd < hi; varEnd++) {
            char c = seq.charAt(varEnd);
            if (c == ' ' || c == '+' || c == '-') {
                break;
            }
        }

        byte varType = parseVarType(seq, lo, varEnd, errorPos);

        // Check for arithmetic operator (skip spaces)
        int opPos = varEnd;
        while (opPos < hi && seq.charAt(opPos) == ' ') {
            opPos++;
        }

        if (opPos >= hi) {
            return encode(varType, (char) 0, 0, false);
        }

        char op = seq.charAt(opPos);
        if (op != '+' && op != '-') {
            throw SqlException.$(errorPos + opPos - lo, "Expected '+' or '-' operator");
        }

        // Parse offset value (skip spaces after operator)
        int offsetStart = opPos + 1;
        while (offsetStart < hi && seq.charAt(offsetStart) == ' ') {
            offsetStart++;
        }

        if (offsetStart >= hi) {
            throw SqlException.$(errorPos + opPos - lo, "Expected number after operator");
        }

        // Find end of number (underscores allowed as separators)
        int numEnd = offsetStart;
        for (; numEnd < hi; numEnd++) {
            char c = seq.charAt(numEnd);
            if (c != '_' && !Chars.isAsciiDigit(c)) {
                break;
            }
        }

        if (numEnd == offsetStart) {
            throw SqlException.$(errorPos + offsetStart - lo, "Expected number after operator");
        }

        int offsetValue;
        try {
            offsetValue = Numbers.parseInt(seq, offsetStart, numEnd);
        } catch (NumericException e) {
            throw SqlException.$(errorPos + offsetStart - lo, "Invalid number in date expression");
        }

        if (op == '-') {
            offsetValue = -offsetValue;
        }

        // Parse unit
        int unitStart = numEnd;
        while (unitStart < hi && seq.charAt(unitStart) == ' ') {
            unitStart++;
        }

        if (unitStart >= hi) {
            throw SqlException.$(errorPos + numEnd - lo, "Expected time unit after number");
        }

        int remaining = hi - unitStart;
        char unitChar = seq.charAt(unitStart);

        // Check for 'bd' (business days) - case insensitive
        if (remaining >= 2 && (unitChar | 32) == 'b' && (seq.charAt(unitStart + 1) | 32) == 'd') {
            if (unitStart + 2 != hi) {
                throw SqlException.$(errorPos + unitStart + 2 - lo, "Unexpected characters after unit");
            }
            return encode(varType, (char) 0, offsetValue, true);
        }

        // Single-character units
        if (unitStart + 1 != hi) {
            throw SqlException.$(errorPos + unitStart + 1 - lo, "Unexpected characters after unit");
        }

        return encode(varType, unitChar, offsetValue, false);
    }

    private static long encode(byte varType, char offsetUnit, int offsetValue, boolean isBusinessDays) {
        return ((long) (varType & 0x3) << 58)
                | (isBusinessDays ? (1L << 57) : 0L)
                | ((long) (offsetUnit & 0xFFFF) << 40)
                | (offsetValue & 0xFFFFFFFFL);
    }

    private static long addBusinessDays(TimestampDriver driver, long timestamp, int businessDays) {
        if (businessDays == 0) {
            return timestamp;
        }

        int direction = businessDays > 0 ? 1 : -1;
        int remaining = Math.abs(businessDays);
        long result = timestamp;

        while (remaining > 0) {
            result = driver.addDays(result, direction);
            int dow = driver.getDayOfWeek(result);
            if (dow != SATURDAY && dow != SUNDAY) {
                remaining--;
            }
        }
        return result;
    }

    private static byte parseVarType(CharSequence seq, int lo, int hi, int errorPos) throws SqlException {
        int varStart = lo + 1; // skip '$'
        int len = hi - varStart;
        switch (len) {
            case 3:
                if ((seq.charAt(varStart) | 32) == 'n'
                        && (seq.charAt(varStart + 1) | 32) == 'o'
                        && (seq.charAt(varStart + 2) | 32) == 'w') {
                    return VAR_NOW;
                }
                break;
            case 5:
                if ((seq.charAt(varStart) | 32) == 't'
                        && (seq.charAt(varStart + 1) | 32) == 'o'
                        && (seq.charAt(varStart + 2) | 32) == 'd'
                        && (seq.charAt(varStart + 3) | 32) == 'a'
                        && (seq.charAt(varStart + 4) | 32) == 'y') {
                    return VAR_TODAY;
                }
                break;
            case 8:
                if ((seq.charAt(varStart) | 32) == 't'
                        && (seq.charAt(varStart + 1) | 32) == 'o'
                        && (seq.charAt(varStart + 2) | 32) == 'm'
                        && (seq.charAt(varStart + 3) | 32) == 'o'
                        && (seq.charAt(varStart + 4) | 32) == 'r'
                        && (seq.charAt(varStart + 5) | 32) == 'r'
                        && (seq.charAt(varStart + 6) | 32) == 'o'
                        && (seq.charAt(varStart + 7) | 32) == 'w') {
                    return VAR_TOMORROW;
                }
                break;
            case 9:
                if ((seq.charAt(varStart) | 32) == 'y'
                        && (seq.charAt(varStart + 1) | 32) == 'e'
                        && (seq.charAt(varStart + 2) | 32) == 's'
                        && (seq.charAt(varStart + 3) | 32) == 't'
                        && (seq.charAt(varStart + 4) | 32) == 'e'
                        && (seq.charAt(varStart + 5) | 32) == 'r'
                        && (seq.charAt(varStart + 6) | 32) == 'd'
                        && (seq.charAt(varStart + 7) | 32) == 'a'
                        && (seq.charAt(varStart + 8) | 32) == 'y') {
                    return VAR_YESTERDAY;
                }
                break;
        }
        throw SqlException.$(errorPos, "Unknown date variable: ").put(seq.subSequence(lo, hi));
    }
}
