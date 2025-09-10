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
import io.questdb.std.Decimal128;
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
     * @param position the position in the SQL query for error reporting
     * @param tok token containing the literal
     * @param decimal to parse and store the resulting value
     * @param precision of the decimal (or -1 to infer from literal)
     * @param scale of the decimal (or -1 to infer from literal)
     * @return a ConstantFunction containing the value parsed
     * @throws SqlException if the value couldn't be parsed with detailed error message
     */
    public static @NotNull ConstantFunction parseDecimalConstant(int position, Decimal256 decimal, final CharSequence tok, int precision, int scale) throws SqlException {
        try {
            precision = decimal.ofString(tok, precision, scale);
        } catch (NumericException ex) {
            throw SqlException.position(position).put(ex);
        }

        return createDecimalConstant(decimal, precision, decimal.getScale());
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
     * Creates a new constant Decimal from the given 256-bit decimal value.
     */
    public static @NotNull ConstantFunction createDecimalConstant(Decimal256 d, int precision, int scale) {
        return createDecimalConstant(d.getHh(), d.getHl(), d.getLh(), d.getLl(), precision, scale);
    }

    /**
     * Creates a new null constant Decimal.
     */
    public static @NotNull ConstantFunction createNullDecimalConstant(int precision, int scale) {
        int type = ColumnType.getDecimalType(precision, scale);
        switch (ColumnType.tagOf(type)) {
            case ColumnType.DECIMAL8:
                return Decimal8Constant.NULL;
            case ColumnType.DECIMAL16:
                return Decimal16Constant.NULL;
            case ColumnType.DECIMAL32:
                return Decimal32Constant.NULL;
            case ColumnType.DECIMAL64:
                return Decimal64Constant.NULL;
            case ColumnType.DECIMAL128:
                return Decimal128Constant.NULL;
            default:
                return Decimal256Constant.NULL;
        }
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
                    .put("invalid DECIMAL type, precision must be a number");
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
                    .put("invalid DECIMAL type, scale must be a number");
        }
    }
}
