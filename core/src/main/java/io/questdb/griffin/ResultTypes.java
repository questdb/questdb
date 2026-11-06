/*+*****************************************************************************
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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.std.Decimals;
import io.questdb.std.FiberLocal;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

import java.math.RoundingMode;

/**
 * Argument-type rules that function factories share to declare {@link FunctionFactory#getResultType} and to type
 * the functions they build.
 */
public final class ResultTypes {
    private static final FiberLocal<IntList> tlArgTypes = new FiberLocal<>(IntList::new);

    private ResultTypes() {
    }

    /**
     * The types of {@code args}, for a factory to apply its result-type rule to the arguments it builds with, in a
     * scratch list of the fiber that the next call on the fiber overwrites.
     */
    public static IntList argTypes(ObjList<Function> args) {
        final IntList argTypes = tlArgTypes.get();
        argTypes.clear();
        for (int i = 0, n = args.size(); i < n; i++) {
            argTypes.add(args.getQuick(i).getType());
        }
        return argTypes;
    }

    /**
     * The type of AND and OR: BOOLEAN over BOOLEAN operands, otherwise {@link ColumnType#UNDEFINED}, because a
     * constant operand folds the call into the other operand, which keeps its own type.
     */
    public static int booleanConnective(int leftType, int rightType) {
        return leftType == ColumnType.BOOLEAN && rightType == ColumnType.BOOLEAN ? ColumnType.BOOLEAN : ColumnType.UNDEFINED;
    }

    /**
     * The target type of {@code cast(value as target)}: the type of its type-constant argument.
     */
    public static int castTarget(IntList argTypes) {
        return argTypes.getQuick(1);
    }

    /**
     * The decimal type of a sum or difference: one integer digit more than the wider operand, the larger scale.
     */
    public static int decimalAddition(int leftType, int rightType) {
        return decimalWithExtraDigits(leftType, rightType, 1);
    }

    /**
     * The decimal type of a product or quotient: maximum precision, the operands' scales added up.
     */
    public static int decimalProduct(int leftType, int rightType) {
        return ColumnType.getDecimalType(
                Decimals.MAX_PRECISION,
                Math.min(ColumnType.getDecimalScale(leftType) + ColumnType.getDecimalScale(rightType), Decimals.MAX_SCALE)
        );
    }

    /**
     * The type of a decimal rounded at a constant number of decimal places, negative to round left of the point: the
     * decimal's own type when it has no more decimal places, otherwise its integer digits plus a carry digit for every
     * rounding mode except {@link RoundingMode#DOWN}, at least one digit, and the decimal places kept, none left of
     * the point.
     */
    public static int decimalRounded(int argType, int roundingScale, RoundingMode roundingMode) {
        return roundingScale >= ColumnType.getDecimalScale(argType)
                ? argType
                : decimalRoundedAt(argType, Math.max(roundingScale, 0), roundingMode);
    }

    /**
     * The type of a decimal rounded at a number of decimal places known only at run time: its digits plus a carry
     * digit for every rounding mode except {@link RoundingMode#DOWN}, and its decimal places.
     */
    public static int decimalRoundedAtRuntime(int argType, RoundingMode roundingMode) {
        return decimalRoundedAt(argType, ColumnType.getDecimalScale(argType), roundingMode);
    }

    /**
     * The decimal type of {@code sum()} over a decimal: the precision of the next wider storage, the argument's scale.
     */
    public static int decimalSum(int argType) {
        final int precision = switch (ColumnType.tagOf(argType)) {
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16 -> Decimals.getDecimalTagPrecision(ColumnType.DECIMAL64);
            case ColumnType.DECIMAL32, ColumnType.DECIMAL64 -> Decimals.getDecimalTagPrecision(ColumnType.DECIMAL128);
            default -> Decimals.MAX_PRECISION;
        };
        return ColumnType.getDecimalType(precision, ColumnType.getDecimalScale(argType));
    }

    /**
     * The decimal type that holds the values of both decimal types: the wider integer part and the larger scale.
     */
    public static int decimalUnion(int leftType, int rightType) {
        return decimalWithExtraDigits(leftType, rightType, 0);
    }

    /**
     * The type of an element-wise operation over two DOUBLE arrays: the higher dimensionality, or weak dimensions
     * while either operand's dimensionality is unknown.
     */
    public static int doubleArrayBroadcast(int leftType, int rightType) {
        final int leftDims = ColumnType.decodeWeakArrayDimensionality(leftType);
        final int rightDims = ColumnType.decodeWeakArrayDimensionality(rightType);
        if (leftDims > 0 && rightDims > 0) {
            return ColumnType.encodeArrayType(ColumnType.DOUBLE, Math.max(leftDims, rightDims));
        }
        return ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, true);
    }

    /**
     * The type of an element-wise aggregate across DOUBLE array arguments of one shape: the dimensionality every
     * argument with a known one shares, or weak dimensions while no argument's dimensionality is known;
     * {@link ColumnType#UNDEFINED} when an argument is not an array or two known dimensionalities differ.
     */
    public static int doubleArrayElementwise(IntList argTypes) {
        int dims = 0;
        for (int i = 0, n = argTypes.size(); i < n; i++) {
            final int type = argTypes.getQuick(i);
            if (!ColumnType.isArray(type)) {
                return ColumnType.UNDEFINED;
            }
            final int argDims = ColumnType.decodeWeakArrayDimensionality(type);
            if (argDims > 0) {
                if (dims > 0 && dims != argDims) {
                    return ColumnType.UNDEFINED;
                }
                dims = argDims;
            }
        }
        return dims > 0 ? ColumnType.encodeArrayType(ColumnType.DOUBLE, dims) : ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, true);
    }

    /**
     * True when GREATEST and LEAST take an argument of the type: a number, a date, a timestamp or NULL.
     */
    public static boolean isNumericExtremeArgument(int type) {
        final short tag = ColumnType.tagOf(type);
        return tag == ColumnType.NULL || (ColumnType.isDecimal(type) && tag != ColumnType.DECIMAL) || extremeRank(tag) > 0;
    }

    /**
     * The type {@code greatest()} and {@code least()} produce over numeric and temporal arguments: DOUBLE or FLOAT
     * when present, otherwise the decimal holding every argument, otherwise the most precise temporal type, otherwise
     * the widest integer type; NULL when every argument is NULL.
     */
    public static int numericExtreme(IntList argTypes) {
        boolean hasDecimal = false;
        boolean hasNonNull = false;
        int widest = ColumnType.UNDEFINED;
        int widestRank = -1;
        for (int i = 0, n = argTypes.size(); i < n; i++) {
            final int type = argTypes.getQuick(i);
            if (ColumnType.isNull(type)) {
                continue;
            }
            hasNonNull = true;
            hasDecimal |= ColumnType.isDecimal(type);
            final int rank = extremeRank(type);
            if (rank > widestRank) {
                widestRank = rank;
                widest = type;
            }
        }
        if (!hasNonNull) {
            return ColumnType.NULL;
        }
        if (hasDecimal && widestRank < extremeRank(ColumnType.FLOAT)) {
            int precision = 1;
            int scale = 0;
            for (int i = 0, n = argTypes.size(); i < n; i++) {
                final int r = DecimalUtil.getTypePrecisionScale(argTypes.getQuick(i));
                final int argScale = Numbers.decodeHighShort(r);
                final int finalScale = Math.max(scale, argScale);
                precision = Math.max(precision - scale, Numbers.decodeLowShort(r) - argScale) + finalScale;
                scale = finalScale;
            }
            return ColumnType.getDecimalType(Math.min(precision, Decimals.MAX_PRECISION), scale);
        }
        return widest;
    }

    private static int decimalRoundedAt(int argType, int scale, RoundingMode roundingMode) {
        final int carry = roundingMode == RoundingMode.DOWN ? 0 : 1;
        final int precision = ColumnType.getDecimalPrecision(argType) - ColumnType.getDecimalScale(argType) + scale + carry;
        return ColumnType.getDecimalType(Math.max(1, Math.min(precision, Decimals.MAX_PRECISION)), scale);
    }

    private static int decimalWithExtraDigits(int leftType, int rightType, int extraDigits) {
        final int leftScale = ColumnType.getDecimalScale(leftType);
        final int rightScale = ColumnType.getDecimalScale(rightType);
        final int scale = Math.max(leftScale, rightScale);
        final int precision = Math.min(
                Math.max(ColumnType.getDecimalPrecision(leftType) - leftScale, ColumnType.getDecimalPrecision(rightType) - rightScale) + scale + extraDigits,
                Decimals.MAX_PRECISION
        );
        return ColumnType.getDecimalType(precision, scale);
    }

    private static int extremeRank(int type) {
        return switch (type) {
            case ColumnType.DOUBLE -> 9;
            case ColumnType.FLOAT -> 8;
            case ColumnType.TIMESTAMP_NANO -> 7;
            case ColumnType.TIMESTAMP_MICRO -> 6;
            case ColumnType.DATE -> 5;
            case ColumnType.LONG -> 4;
            case ColumnType.INT -> 3;
            case ColumnType.SHORT -> 2;
            case ColumnType.BYTE -> 1;
            default -> 0;
        };
    }
}
