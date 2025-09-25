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
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.engine.functions.cast.CastByteToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDecimalToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class DecimalUtil {
    public static final int BYTE_TYPE = ColumnType.getDecimalType(3, 0);
    public static final int INT_TYPE = ColumnType.getDecimalType(10, 0);
    public static final int LONG_TYPE = ColumnType.getDecimalType(19, 0);
    public static final int SHORT_TYPE = ColumnType.getDecimalType(5, 0);

    private DecimalUtil() {
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
    public static @NotNull ConstantFunction createDecimalConstant(@NotNull Decimal256 d, int precision, int scale) {
        return createDecimalConstant(d.getHh(), d.getHl(), d.getLh(), d.getLl(), precision, scale);
    }

    /**
     * Creates a new null constant Decimal.
     */
    public static @NotNull ConstantFunction createNullDecimalConstant(int precision, int scale) {
        int type = ColumnType.getDecimalType(precision, scale);
        switch (ColumnType.tagOf(type)) {
            case ColumnType.DECIMAL8:
                return new Decimal8Constant(Decimals.DECIMAL8_NULL, type);
            case ColumnType.DECIMAL16:
                return new Decimal16Constant(Decimals.DECIMAL16_NULL, type);
            case ColumnType.DECIMAL32:
                return new Decimal32Constant(Decimals.DECIMAL32_NULL, type);
            case ColumnType.DECIMAL64:
                return new Decimal64Constant(Decimals.DECIMAL64_NULL, type);
            case ColumnType.DECIMAL128:
                return new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, type);
            default:
                return new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL,
                        type
                );
        }
    }

    /**
     * Returns a function that can cast a type to a specific decimal through implicit casting.
     * It may return null if the type cannot be implicitly cast.
     */
    public static Function getImplicitCastFunction(Function arg, int position, int toType, SqlExecutionContext sqlExecutionContext) throws SqlException {
        switch (ColumnType.tagOf(arg.getType())) {
            case ColumnType.DECIMAL8:
            case ColumnType.DECIMAL16:
            case ColumnType.DECIMAL32:
            case ColumnType.DECIMAL64:
            case ColumnType.DECIMAL128:
            case ColumnType.DECIMAL256:
                return CastDecimalToDecimalFunctionFactory.newInstance(position, arg, toType, sqlExecutionContext);
            case ColumnType.BYTE:
                return CastByteToDecimalFunctionFactory.newInstance(position, arg, toType, sqlExecutionContext);
            case ColumnType.SHORT:
                return CastShortToDecimalFunctionFactory.newInstance(position, arg, toType, sqlExecutionContext);
            case ColumnType.INT:
                return CastIntToDecimalFunctionFactory.newInstance(position, arg, toType, sqlExecutionContext);
            case ColumnType.LONG:
                return CastLongToDecimalFunctionFactory.newInstance(position, arg, toType, sqlExecutionContext);
        }
        return null;
    }

    /**
     * Returns a decimal type that is fitting for the original type or 0 if it cannot be implicitly cast.
     */
    public static int getImplicitCastType(int fromType) {
        if (ColumnType.isDecimal(fromType)) {
            return fromType;
        }
        switch (fromType) {
            case ColumnType.LONG:
                return ColumnType.getDecimalType(Numbers.getPrecision(Long.MAX_VALUE), 0);
            case ColumnType.INT:
                return ColumnType.getDecimalType(Numbers.getPrecision(Integer.MAX_VALUE), 0);
            case ColumnType.SHORT:
                return ColumnType.getDecimalType(Numbers.getPrecision(Short.MAX_VALUE), 0);
            case ColumnType.BYTE:
                return ColumnType.getDecimalType(Numbers.getPrecision(Byte.MAX_VALUE), 0);
        }
        return 0;
    }

    /**
     * Load any decimal value from a Function into a Decimal256
     */
    public static void load(Decimal256 decimal, Function value, @Nullable Record rec) {
        final int fromType = value.getType();
        load(decimal, value, rec, fromType);
    }

    /**
     * Load any decimal value from a Function into a Decimal256
     */
    public static void load(Decimal256 decimal, Function value, @Nullable Record rec, int fromType) {
        final int fromPrecision = ColumnType.getDecimalPrecision(fromType);
        final int fromScale = ColumnType.getDecimalScale(fromType);

        switch (Decimals.getStorageSizePow2(fromPrecision)) {
            case 0:
                byte b = value.getDecimal8(rec);
                if (b == Decimals.DECIMAL8_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(b, fromScale);
                }
                break;
            case 1:
                short s = value.getDecimal16(rec);
                if (s == Decimals.DECIMAL16_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(s, fromScale);
                }
                break;
            case 2:
                int i = value.getDecimal32(rec);
                if (i == Decimals.DECIMAL32_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(i, fromScale);
                }
                break;
            case 3:
                long l = value.getDecimal64(rec);
                if (l == Decimals.DECIMAL64_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(l, fromScale);
                }
                break;
            case 4:
                long hi = value.getDecimal128Hi(rec);
                long lo = value.getDecimal128Lo(rec);
                if (Decimal128.isNull(hi, lo)) {
                    decimal.ofNull();
                } else {
                    long v = hi < 0 ? -1 : 0;
                    decimal.of(v, v, hi, lo, fromScale);
                }
                break;
            default:
                long hh = value.getDecimal256HH(rec);
                long hl = value.getDecimal256HL(rec);
                long lh = value.getDecimal256LH(rec);
                long ll = value.getDecimal256LL(rec);
                decimal.of(hh, hl, lh, ll, fromScale);
                break;
        }
    }

    /**
     * Load a decimal value from a Record into a Decimal256
     */
    public static void load(Decimal256 decimal, @NotNull Record rec, int col, int fromType) {
        final int fromPrecision = ColumnType.getDecimalPrecision(fromType);
        final int fromScale = ColumnType.getDecimalScale(fromType);

        switch (Decimals.getStorageSizePow2(fromPrecision)) {
            case 0:
                byte b = rec.getDecimal8(col);
                if (b == Decimals.DECIMAL8_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(b, fromScale);
                }
                break;
            case 1:
                short s = rec.getDecimal16(col);
                if (s == Decimals.DECIMAL16_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(s, fromScale);
                }
                break;
            case 2:
                int i = rec.getDecimal32(col);
                if (i == Decimals.DECIMAL32_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(i, fromScale);
                }
                break;
            case 3:
                long l = rec.getDecimal64(col);
                if (l == Decimals.DECIMAL64_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(l, fromScale);
                }
                break;
            case 4:
                long hi = rec.getDecimal128Hi(col);
                long lo = rec.getDecimal128Lo(col);
                if (Decimal128.isNull(hi, lo)) {
                    decimal.ofNull();
                } else {
                    long v = hi < 0 ? -1 : 0;
                    decimal.of(v, v, hi, lo, fromScale);
                }
                break;
            default:
                long hh = rec.getDecimal256HH(col);
                long hl = rec.getDecimal256HL(col);
                long lh = rec.getDecimal256LH(col);
                long ll = rec.getDecimal256LL(col);
                decimal.of(hh, hl, lh, ll, fromScale);
                break;
        }
    }

    /**
     * Load a decimal value from a Function into a Decimal64
     */
    public static void load(Decimal64 decimal, Function value, @Nullable Record rec) {
        final int fromType = value.getType();
        final int fromPrecision = ColumnType.getDecimalPrecision(fromType);
        final int fromScale = ColumnType.getDecimalScale(fromType);

        switch (Decimals.getStorageSizePow2(fromPrecision)) {
            case 0:
                byte b = value.getDecimal8(rec);
                if (b == Decimals.DECIMAL8_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.of(b, fromScale);
                }
                break;
            case 1:
                short s = value.getDecimal16(rec);
                if (s == Decimals.DECIMAL16_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.of(s, fromScale);
                }
                break;
            case 2:
                int i = value.getDecimal32(rec);
                if (i == Decimals.DECIMAL32_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.of(i, fromScale);
                }
                break;
            case 3:
                long l = value.getDecimal64(rec);
                if (l == Decimals.DECIMAL64_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.of(l, fromScale);
                }
                break;
            default:
                throw new UnsupportedOperationException("Too large precision for Decimal64:" + fromPrecision);
        }
    }

    /**
     * Load a decimal value from a Function into a Decimal128
     */
    public static void load(Decimal128 decimal, Function value, @Nullable Record rec) {
        final int fromType = value.getType();
        final int fromPrecision = ColumnType.getDecimalPrecision(fromType);
        final int fromScale = ColumnType.getDecimalScale(fromType);

        switch (Decimals.getStorageSizePow2(fromPrecision)) {
            case 0:
                byte b = value.getDecimal8(rec);
                if (b == Decimals.DECIMAL8_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(b, fromScale);
                }
                break;
            case 1:
                short s = value.getDecimal16(rec);
                if (s == Decimals.DECIMAL16_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(s, fromScale);
                }
                break;
            case 2:
                int i = value.getDecimal32(rec);
                if (i == Decimals.DECIMAL32_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(i, fromScale);
                }
                break;
            case 3:
                long l = value.getDecimal64(rec);
                if (l == Decimals.DECIMAL64_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(l, fromScale);
                }
                break;
            case 4:
                long hi = value.getDecimal128Hi(rec);
                long lo = value.getDecimal128Lo(rec);
                if (Decimal128.isNull(hi, lo)) {
                    decimal.ofNull();
                } else {
                    decimal.of(hi, lo, fromScale);
                }
                break;
            default:
                throw new UnsupportedOperationException("Too large precision for Decimal128:" + fromPrecision);
        }
    }

    /**
     * Checks if the provided function is a Decimal constant and, if so, tries to rescale it.
     * Otherwise, returns the same function.
     * <p>
     * This method is useful when choosing the most efficient function implementation in a function factory.
     * After rescaling one of the arguments, we no longer have to rescale it for each row.
     *
     * @param func            the input function; must not be used after this call as it may be closed.
     * @param decimal         intermediate object, used for rescaling
     * @param targetPrecision target precision
     * @param targetScale     target scale
     * @return the newly created Decimal constant function
     */
    public static @NotNull Function maybeRescaleDecimalConstant(
            @NotNull Function func,
            @NotNull Decimal256 decimal,
            int targetPrecision,
            int targetScale
    ) {
        final int type = func.getType();
        final int scale = ColumnType.getDecimalScale(type);
        if (func.isConstant() && ColumnType.isDecimal(type) && scale < targetScale) {
            final int rescaledPrecision = ColumnType.getDecimalPrecision(type) + (targetScale - scale);
            if (rescaledPrecision <= Decimals.MAX_PRECISION) {
                // aim for the target precision
                targetPrecision = Math.max(targetPrecision, rescaledPrecision);
                try {
                    final Function newFunc;
                    if (func.isNullConstant()) {
                        newFunc = createNullDecimalConstant(targetPrecision, targetScale);
                    } else {
                        load(decimal, func, null);
                        decimal.rescale(targetScale);
                        newFunc = createDecimalConstant(decimal, targetPrecision, targetScale);
                    }
                    func.close();
                    return newFunc;
                } catch (NumericException ignore) {
                    // no-op, just return the func as is
                }
            }
        }
        return func;
    }

    /**
     * Parses a decimal from a literal to the most adapted decimal type as a constant.
     * The literal may end with [m/M] but not necessarily.
     *
     * @param position  the position in the SQL query for error reporting
     * @param tok       token containing the literal
     * @param decimal   to parse and store the resulting value
     * @param precision of the decimal (or -1 to infer from literal)
     * @param scale     of the decimal (or -1 to infer from literal)
     * @return a ConstantFunction containing the value parsed
     * @throws SqlException if the value couldn't be parsed with detailed error message
     */
    public static @NotNull ConstantFunction parseDecimalConstant(
            int position,
            @NotNull Decimal256 decimal,
            @NotNull CharSequence tok,
            int precision,
            int scale
    ) throws SqlException {
        try {
            int p = Numbers.decodeLowInt(decimal.ofString(tok, precision, scale));
            precision = precision == -1 ? p : precision;
        } catch (NumericException ex) {
            throw SqlException.position(position).put(ex);
        }

        return createDecimalConstant(decimal, precision, decimal.getScale());
    }

    /**
     * Parses the precision from a CharSequence and returns its value, it doesn't validate whether
     * the value is in the correct range.
     *
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
     *
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

    /**
     * Store a Decimal256 to a memory location
     */
    public static void store(Decimal256 decimal, MemoryA mem, int targetType) {
        if (decimal.isNull()) {
            switch (ColumnType.tagOf(targetType)) {
                case ColumnType.DECIMAL8:
                    mem.putByte(Decimals.DECIMAL8_NULL);
                    break;
                case ColumnType.DECIMAL16:
                    mem.putShort(Decimals.DECIMAL16_NULL);
                    break;
                case ColumnType.DECIMAL32:
                    mem.putInt(Decimals.DECIMAL32_NULL);
                    break;
                case ColumnType.DECIMAL64:
                    mem.putLong(Decimals.DECIMAL64_NULL);
                    break;
                case ColumnType.DECIMAL128:
                    mem.putDecimal128(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
                    break;
                case ColumnType.DECIMAL256:
                    mem.putDecimal256(
                            Decimals.DECIMAL256_HH_NULL,
                            Decimals.DECIMAL256_HL_NULL,
                            Decimals.DECIMAL256_LH_NULL,
                            Decimals.DECIMAL256_LL_NULL
                    );
                    break;
                default:
                    assert false;
            }
            return;
        }
        switch (ColumnType.tagOf(targetType)) {
            case ColumnType.DECIMAL8:
                mem.putByte((byte) decimal.getLl());
                break;
            case ColumnType.DECIMAL16:
                mem.putShort((short) decimal.getLl());
                break;
            case ColumnType.DECIMAL32:
                mem.putInt((int) decimal.getLl());
                break;
            case ColumnType.DECIMAL64:
                mem.putLong(decimal.getLl());
                break;
            case ColumnType.DECIMAL128:
                mem.putDecimal128(decimal.getLh(), decimal.getLl());
                break;
            default:
                mem.putDecimal256(
                        decimal.getHh(),
                        decimal.getHl(),
                        decimal.getLh(),
                        decimal.getLl()
                );
                break;
        }
    }

    /**
     * Store a non-null Decimal256 to a row
     */
    public static void store(Decimal256 decimal, TableWriter.Row row, int columnIndex, int targetType) {
        if (decimal.isNull()) {
            storeNull(row, columnIndex, targetType);
        } else {
            storeNonNull(decimal, row, columnIndex, targetType);
        }
    }

    /**
     * Store a non-null Decimal256 to a row
     */
    public static void storeNonNull(Decimal256 decimal, TableWriter.Row row, int columnIndex, int targetType) {
        switch (ColumnType.tagOf(targetType)) {
            case ColumnType.DECIMAL8:
                row.putByte(columnIndex, (byte) decimal.getLl());
                break;
            case ColumnType.DECIMAL16:
                row.putShort(columnIndex, (short) decimal.getLl());
                break;
            case ColumnType.DECIMAL32:
                row.putInt(columnIndex, (int) decimal.getLl());
                break;
            case ColumnType.DECIMAL64:
                row.putLong(columnIndex, decimal.getLl());
                break;
            case ColumnType.DECIMAL128:
                row.putDecimal128(columnIndex, decimal.getLh(), decimal.getLl());
                break;
            default:
                row.putDecimal256(
                        columnIndex,
                        decimal.getHh(),
                        decimal.getHl(),
                        decimal.getLh(),
                        decimal.getLl()
                );
                break;
        }
    }

    /**
     * Store a Decimal256 to a row
     */
    public static void storeNull(TableWriter.Row row, int columnIndex, int targetType) {
        switch (ColumnType.tagOf(targetType)) {
            case ColumnType.DECIMAL8:
                row.putByte(columnIndex, Decimals.DECIMAL8_NULL);
                break;
            case ColumnType.DECIMAL16:
                row.putShort(columnIndex, Decimals.DECIMAL16_NULL);
                break;
            case ColumnType.DECIMAL32:
                row.putInt(columnIndex, Decimals.DECIMAL32_NULL);
                break;
            case ColumnType.DECIMAL64:
                row.putLong(columnIndex, Decimals.DECIMAL64_NULL);
                break;
            case ColumnType.DECIMAL128:
                row.putDecimal128(columnIndex, Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
                break;
            default:
                row.putDecimal256(
                        columnIndex,
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL
                );
                break;
        }
    }
}
