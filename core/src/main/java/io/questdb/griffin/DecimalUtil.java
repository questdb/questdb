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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import org.jetbrains.annotations.NotNull;

public final class DecimalUtil {
    private DecimalUtil() {}

    /**
     * Parses a decimal from a literal to the most adapted decimal type as a constant.
     * The literal may end with [m/M] but not necessarily.
     * @param tok token containing the literal
     * @param len length of the token
     * @return a ConstantFunction containing the value parsed
     * @throws NumericException if the value couldn't be parsed
     */
    public static @NotNull ConstantFunction parseDecimalConstant(final CharSequence tok, final int len) throws NumericException {
        int lo = 0;
        int hi = len;

        // We don't want to parse the m suffix, we can safely skip it
        if (hi > 0 && (tok.charAt(hi - 1) == 'm' || tok.charAt(hi - 1) == 'M')) {
            hi--;
        }

        while (lo < hi && tok.charAt(lo) == ' ') {
            lo++;
        }

        if (lo == hi) {
            throw NumericException.instance();
        }

        // Parses sign
        boolean negative = false;
        if (tok.charAt(lo) == '-') {
            negative = true;
            lo++;
        } else if (tok.charAt(lo) == '+') {
            lo++;
        }

        while (lo < hi && tok.charAt(lo) == '0') {
            lo++;
        }

        if (lo == hi) {
            throw NumericException.instance();
        }

        // We do a first pass over the literal to ensure that the format is correct (numerical and at most 1 dot) and to
        // measure the precision/scale.
        int dot = validateDecimalLiteral(tok, lo, hi);
        int precision = hi - lo - (dot == -1 ? 0 : 1);
        if (precision > Decimals.MAX_PRECISION) {
            throw NumericException.instance();
        }
        int scale = dot == -1 ? 0 : (hi - dot - 1);

        long hh = 0L;
        long hl = 0L;
        long lh = 0L;
        long ll = 0L;
        // Actual parsing of the number on a second pass, we're relying on Decimal256 to do
        // the proper scaling.
        int pow = precision;
        for (int p = lo; p < hi; p++) {
            if (p == dot) {
                continue;
            }

            pow--;
            int times = tok.charAt(p) - '0';
            if (times == 0) {
                continue;
            }

            final long multiplierHH = pow >= 58 ? Decimal256.POWERS_TEN_TABLE_HH[pow - 58] : 0L;
            final long multiplierHL = pow >= 39 ? Decimal256.POWERS_TEN_TABLE_HL[pow - 39] : 0L;
            final long multiplierLH = pow >= 20 ? Decimal256.POWERS_TEN_TABLE_LH[pow - 20] : 0L;
            final long multiplierLL = Decimal256.POWERS_TEN_TABLE_LL[pow];
            // TODO: replace loop with all possibilities for each power (76 * 9 cases)
            for (int i = 0; i < times; i++) {
                long r = ll + multiplierLL;
                long carry = Long.compareUnsigned(r, ll) < 0 ? 1L : 0L;
                ll = r;

                long t = lh + carry;
                carry = Long.compareUnsigned(t, lh) < 0 ? 1L : 0L;
                r = t + multiplierLH;
                carry |= Long.compareUnsigned(r, t) < 0 ? 1L : 0L;
                lh = r;

                t = hl + carry;
                carry = Long.compareUnsigned(t, hl) < 0 ? 1L : 0L;
                r = t + multiplierHL;
                carry |= Long.compareUnsigned(r, t) < 0 ? 1L : 0L;
                hl = r;

                // No need to check for overflow as we already know the precision
                hh = hh + multiplierHH + carry;
            }
        }

        if (negative) {
            ll = ~ll + 1;
            long c = ll == 0L ? 1L : 0L;
            lh = ~lh + c;
            c = (c == 1L && lh == 0L) ? 1L : 0L;
            hl = ~hl + c;
            c = (c == 1L && hl == 0L) ? 1L : 0L;
            hh = ~hh + c;
        }

        return createDecimalConstant(hh, hl, lh, ll, precision, scale);
    }

    /**
     * Creates a new constant Decimal from the given 256-bit decimal value.
     */
    public static @NotNull ConstantFunction createDecimalConstant(long hh, long hl, long lh, long ll, int precision, int scale) {
        int type = ColumnType.getDecimalType(precision, scale);
        switch (ColumnType.tagOf(type)) {
            case ColumnType.DECIMAL8:
                return new Decimal8Constant((byte) ll, type);
            case ColumnType.DECIMAL16:
                return new Decimal16Constant((short) ll, type);
            case ColumnType.DECIMAL32:
                return new Decimal32Constant((int) ll, type);
            case ColumnType.DECIMAL64:
                return new Decimal64Constant(ll, type);
            case ColumnType.DECIMAL128:
                return new Decimal128Constant(lh, ll, type);
            default:
                return new Decimal256Constant(hh, hl, lh, ll, type);
        }
    }

    /**
     * Pass over a literal and validates that it only contains digits and at most 1 dot.
     * If a dot is found, it returns its position, -1 otherwise.
     */
    private static int validateDecimalLiteral(CharSequence tok, int lo, int hi) throws NumericException {
        int dot = -1;
        boolean digitFound = false;
        for (int p = lo; p < hi; p++) {
            char c  = tok.charAt(p);
            if (isDigit(c)) {
                digitFound = true;
                continue;
            } else if (c == '.' && dot == -1) {
                dot = p;
                continue;
            }
            throw NumericException.instance();
        }
        if (!digitFound) {
            throw NumericException.instance();
        }
        return dot;
    }

    private static boolean isDigit(char c) {
        return '0' <= c && c <= '9';
    }

    /**
     * Parses the precision from a CharSequence and returns its value, it doesn't validate whether
     * the value is in the correct range.
     * @throws SqlException if the value couldn't be parsed
     */
    public static int parsePrecision(int position, @NotNull CharSequence cs, int lo, int hi) throws SqlException {
        try {
            return Numbers.parseInt(cs, lo, hi);
        } catch (NumericException e) {
            throw SqlException.position(position)
                    .put("invalid DECIMAL precision, must be a number");
        }
    }

    /**
     * Parses the scale from a CharSequence and returns its value, it doesn't validate whether
     * the value is in the correct range.
     * @throws SqlException if the value couldn't be parsed
     */
    public static int parseScale(int position, @NotNull CharSequence cs, int lo, int hi) throws SqlException {
        try {
            return Numbers.parseInt(cs, lo, hi);
        } catch (NumericException e) {
            throw SqlException.position(position)
                    .put("invalid DECIMAL scale, must be a number");
        }
    }
}
