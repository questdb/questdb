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
import io.questdb.std.Decimal;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DecimalParser;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

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
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.DECIMAL8 -> new Decimal8Constant((byte) ll, type);
            case ColumnType.DECIMAL16 -> new Decimal16Constant((short) ll, type);
            case ColumnType.DECIMAL32 -> new Decimal32Constant((int) ll, type);
            case ColumnType.DECIMAL64 -> new Decimal64Constant(ll, type);
            case ColumnType.DECIMAL128 -> new Decimal128Constant(lh, ll, type);
            default -> new Decimal256Constant(hh, hl, lh, ll, type);
        };
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
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.DECIMAL8 -> new Decimal8Constant(Decimals.DECIMAL8_NULL, type);
            case ColumnType.DECIMAL16 -> new Decimal16Constant(Decimals.DECIMAL16_NULL, type);
            case ColumnType.DECIMAL32 -> new Decimal32Constant(Decimals.DECIMAL32_NULL, type);
            case ColumnType.DECIMAL64 -> new Decimal64Constant(Decimals.DECIMAL64_NULL, type);
            case ColumnType.DECIMAL128 ->
                    new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, type);
            default -> new Decimal256Constant(
                    Decimals.DECIMAL256_HH_NULL,
                    Decimals.DECIMAL256_HL_NULL,
                    Decimals.DECIMAL256_LH_NULL,
                    Decimals.DECIMAL256_LL_NULL,
                    type
            );
        };
    }

    /**
     * Returns a decimal instance from a sqlExecutionContext that can fit any number of {@code precision} digits.
     *
     * @param executionContext to retrieve an instance of the decimal
     */
    @TestOnly
    public static @NotNull Decimal getDecimal(SqlExecutionContext executionContext, int precision) {
        return switch (Decimals.getStorageSizePow2(precision)) {
            case 0, 1, 2, 3 -> executionContext.getDecimal64();
            case 4 -> executionContext.getDecimal128();
            default -> executionContext.getDecimal256();
        };
    }

    /**
     * Returns a function that can cast a type to a specific decimal through implicit casting.
     * It may return null if the type cannot be implicitly cast.
     */
    public static Function getImplicitCastFunction(Function arg, int position, int toType, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return switch (ColumnType.tagOf(arg.getType())) {
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64,
                 ColumnType.DECIMAL128, ColumnType.DECIMAL256 ->
                    CastDecimalToDecimalFunctionFactory.newInstance(position, arg, toType, sqlExecutionContext);
            case ColumnType.BYTE ->
                    CastByteToDecimalFunctionFactory.newInstance(position, arg, toType, sqlExecutionContext);
            case ColumnType.SHORT ->
                    CastShortToDecimalFunctionFactory.newInstance(position, arg, toType, sqlExecutionContext);
            case ColumnType.INT ->
                    CastIntToDecimalFunctionFactory.newInstance(position, arg, toType, sqlExecutionContext);
            case ColumnType.LONG ->
                    CastLongToDecimalFunctionFactory.newInstance(position, arg, toType, sqlExecutionContext.getDecimal256());
            default -> null;
        };
    }

    /**
     * Returns a decimal type that is fitting for the original type or 0 if it cannot be implicitly cast.
     */
    public static int getImplicitCastType(int fromType) {
        if (ColumnType.isDecimal(fromType)) {
            return fromType;
        }
        return switch (fromType) {
            case ColumnType.LONG -> ColumnType.getDecimalType(Numbers.getPrecision(Long.MAX_VALUE), 0);
            case ColumnType.INT -> ColumnType.getDecimalType(Numbers.getPrecision(Integer.MAX_VALUE), 0);
            case ColumnType.SHORT -> ColumnType.getDecimalType(Numbers.getPrecision(Short.MAX_VALUE), 0);
            case ColumnType.BYTE -> ColumnType.getDecimalType(Numbers.getPrecision(Byte.MAX_VALUE), 0);
            default -> 0;
        };
    }

    /**
     * Returns the precision and scale that can accommodate a specific type.
     * Note that all types aren't supported, only those are:
     * - Decimals
     * - Long
     * - Int
     * - Short
     * - Byte
     * - Timestamp
     * - Date
     *
     * @return the precision and scale as low/high short encoded with {@link Numbers#encodeLowHighShorts} or 0
     * if the type is not supported.
     */
    public static int getTypePrecisionScale(int type) {
        int tag = ColumnType.tagOf(type);
        if (ColumnType.isDecimalType(tag)) {
            short p = (short) ColumnType.getDecimalPrecision(type);
            short s = (short) ColumnType.getDecimalScale(type);
            return Numbers.encodeLowHighShorts(p, s);
        }
        return switch (tag) {
            case ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.LONG ->
                    Numbers.encodeLowHighShorts((short) 19, (short) 0);
            case ColumnType.INT -> Numbers.encodeLowHighShorts((short) 10, (short) 0);
            case ColumnType.SHORT -> Numbers.encodeLowHighShorts((short) 5, (short) 0);
            case ColumnType.BYTE -> Numbers.encodeLowHighShorts((short) 3, (short) 0);
            default -> 0;
        };
    }

    /**
     * Load any decimal value from a Function into a Decimal256
     */
    public static void load(Decimal256 decimal, Decimal128 decimal128, Function value, @Nullable Record rec) {
        final int fromType = value.getType();
        load(decimal, decimal128, value, rec, fromType);
    }

    /**
     * Load any decimal value from a Function into a Decimal256
     */
    public static void load(Decimal256 decimal, Decimal128 decimal128, Function value, @Nullable Record rec, int fromType) {
        switch (ColumnType.tagOf(fromType)) {
            case ColumnType.DECIMAL8:
                byte b = value.getDecimal8(rec);
                if (b == Decimals.DECIMAL8_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(b, ColumnType.getDecimalScale(fromType));
                }
                break;
            case ColumnType.DECIMAL16:
                short s = value.getDecimal16(rec);
                if (s == Decimals.DECIMAL16_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(s, ColumnType.getDecimalScale(fromType));
                }
                break;
            case ColumnType.DECIMAL32:
                int i = value.getDecimal32(rec);
                if (i == Decimals.DECIMAL32_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(i, ColumnType.getDecimalScale(fromType));
                }
                break;
            case ColumnType.DECIMAL64:
                long l = value.getDecimal64(rec);
                if (l == Decimals.DECIMAL64_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(l, ColumnType.getDecimalScale(fromType));
                }
                break;
            case ColumnType.DECIMAL128:
                value.getDecimal128(rec, decimal128);
                if (decimal128.isNull()) {
                    decimal.ofNull();
                } else {
                    decimal.of(decimal128, ColumnType.getDecimalScale(fromType));
                }
                break;
            case ColumnType.DECIMAL256:
                value.getDecimal256(rec, decimal);
                decimal.setScale(ColumnType.getDecimalScale(fromType));
                break;
            case ColumnType.LONG: {
                long v = value.getLong(rec);
                if (v == Numbers.LONG_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(v, 0);
                }
                break;
            }
            case ColumnType.DATE: {
                long v = value.getDate(rec);
                if (v == Numbers.LONG_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(v, 0);
                }
                break;
            }
            case ColumnType.TIMESTAMP: {
                long v = value.getTimestamp(rec);
                if (v == Numbers.LONG_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(v, 0);
                }
                break;
            }
            case ColumnType.INT: {
                int v = value.getInt(rec);
                if (v == Numbers.INT_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(v, 0);
                }
                break;
            }
            case ColumnType.SHORT: {
                short v = value.getShort(rec);
                decimal.ofLong(v, 0);
                break;
            }
            case ColumnType.BYTE: {
                byte v = value.getByte(rec);
                decimal.ofLong(v, 0);
                break;
            }
        }
    }

    /**
     * Load a decimal value from a Record into a Decimal256
     */
    public static void load(Decimal256 decimal, Decimal128 decimal128, @NotNull Record rec, int col, int fromType) {
        switch (ColumnType.tagOf(fromType)) {
            case ColumnType.DECIMAL8:
                byte b = rec.getDecimal8(col);
                if (b == Decimals.DECIMAL8_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(b, ColumnType.getDecimalScale(fromType));
                }
                break;
            case ColumnType.DECIMAL16:
                short s = rec.getDecimal16(col);
                if (s == Decimals.DECIMAL16_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(s, ColumnType.getDecimalScale(fromType));
                }
                break;
            case ColumnType.DECIMAL32:
                int i = rec.getDecimal32(col);
                if (i == Decimals.DECIMAL32_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(i, ColumnType.getDecimalScale(fromType));
                }
                break;
            case ColumnType.DECIMAL64:
                long l = rec.getDecimal64(col);
                if (l == Decimals.DECIMAL64_NULL) {
                    decimal.ofNull();
                } else {
                    decimal.ofLong(l, ColumnType.getDecimalScale(fromType));
                }
                break;
            case ColumnType.DECIMAL128:
                rec.getDecimal128(col, decimal128);
                if (decimal128.isNull()) {
                    decimal.ofNull();
                } else {
                    decimal.of(decimal128, ColumnType.getDecimalScale(fromType));
                }
                break;
            default:
                rec.getDecimal256(col, decimal);
                decimal.setScale(ColumnType.getDecimalScale(fromType));
                break;
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
            @NotNull Decimal128 decimal128,
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
                        load(decimal, decimal128, func, null);
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
     * @param position         the position in the SQL query for error reporting
     * @param tok              token containing the literal
     * @param executionContext to retrieve an instance of a decimal that can parse and store the resulting value
     * @param precision        of the decimal (or -1 to infer from literal)
     * @param scale            of the decimal (or -1 to infer from literal)
     * @return a ConstantFunction containing the value parsed
     * @throws SqlException if the value couldn't be parsed with detailed error message
     */
    public static @NotNull ConstantFunction parseDecimalConstant(
            int position,
            @NotNull SqlExecutionContext executionContext,
            @NotNull CharSequence tok,
            int precision,
            int scale
    ) throws SqlException {
        Decimal256 decimal256 = executionContext.getDecimal256();
        try {
            // We might not know the precision of the final type, in this case we want to use
            // the decimal that have the most precision.
            Decimal decimal = precision > 0 ? getDecimal(executionContext, precision) : decimal256;
            long r = DecimalParser.parse(decimal, tok, 0, tok.length(), precision, scale, false, false);
            precision = precision == -1 ? Numbers.decodeLowInt(r) : precision;
            decimal.toDecimal256(decimal256);
        } catch (NumericException ex) {
            throw SqlException.position(position).put(ex);
        }

        return createDecimalConstant(decimal256, precision, decimal256.getScale());
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
                    .put("Invalid decimal type. The precision ('")
                    .put(cs, lo, hi)
                    .put("') must be a number");
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
                    .put("Invalid decimal type. The scale ('")
                    .put(cs, lo, hi)
                    .put("') must be a number");
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
