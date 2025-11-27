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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

public class CastDecimalToDecimalFunctionFactory implements FunctionFactory {

    public static Function newInstance(
            int position,
            Function arg,
            int targetType,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (arg.isConstant()) {
            return newConstantInstance(sqlExecutionContext, position, targetType, arg);
        }

        final int fromType = arg.getType();
        final int fromScale = ColumnType.getDecimalScale(fromType);
        final int targetScale = ColumnType.getDecimalScale(targetType);
        if (fromScale == targetScale) {
            return newUnscaledInstance(position, fromType, targetType, arg);
        }

        return switch (ColumnType.tagOf(fromType)) {
            case ColumnType.DECIMAL8 -> new ScaledDecimal8FuncAbstract(position, targetType, arg, fromScale);
            case ColumnType.DECIMAL16 -> new ScaledDecimal16FuncAbstract(position, targetType, arg, fromScale);
            case ColumnType.DECIMAL32 -> new ScaledDecimal32FuncAbstract(position, targetType, arg, fromScale);
            case ColumnType.DECIMAL64 -> new ScaledDecimal64FuncAbstract(position, targetType, arg, fromScale);
            case ColumnType.DECIMAL128 -> new ScaledDecimal128FuncAbstract(position, targetType, arg, fromScale);
            default -> new ScaledDecimal256FuncAbstract(position, targetType, arg, fromScale);
        };
    }

    @Override
    public String getSignature() {
        return "cast(Ξξ)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final int targetType = args.getQuick(1).getType();
        return newInstance(position, args.getQuick(0), targetType, sqlExecutionContext);
    }

    private static Function newConstantInstance(
            SqlExecutionContext sqlExecutionContext,
            int valuePosition,
            int targetType,
            Function value
    ) {
        final int targetPrecision = ColumnType.getDecimalPrecision(targetType);
        final int targetScale = ColumnType.getDecimalScale(targetType);
        final int fromType = value.getType();
        final int fromScale = ColumnType.getDecimalScale(fromType);

        Decimal256 decimal = sqlExecutionContext.getDecimal256();
        DecimalUtil.load(decimal, sqlExecutionContext.getDecimal128(), value, null);

        if (decimal.isNull()) {
            return DecimalUtil.createNullDecimalConstant(targetPrecision, targetScale);
        }

        long hh = decimal.getHh();
        long hl = decimal.getHl();
        long lh = decimal.getLh();
        long ll = decimal.getLl();
        if (fromScale != targetScale) {
            try {
                decimal.rescale(targetScale);
            } catch (NumericException ignored) {
                decimal.of(hh, hl, lh, ll, fromScale);
                throw ImplicitCastException.inconvertibleValue(decimal, fromType, targetType).position(valuePosition);
            }
        }

        // Ensures that the value fits in the decimal target
        if (!decimal.comparePrecision(targetPrecision)) {
            decimal.of(hh, hl, lh, ll, fromScale);
            throw ImplicitCastException.inconvertibleValue(decimal, fromType, targetType).position(valuePosition);
        }

        // We don't try to narrow the type on its actual precision so that the user may pre-widen the constant decimal
        // to avoid doing it at query execution.
        return DecimalUtil.createDecimalConstant(decimal, targetPrecision, targetScale);
    }

    private static Function newUnscaledInstance(int valuePosition, int fromType, int targetType, Function value) {
        int fromPrecision = ColumnType.getDecimalPrecision(fromType);
        int targetPrecision = ColumnType.getDecimalPrecision(targetType);
        if (targetPrecision >= fromPrecision) {
            return switch (ColumnType.tagOf(fromType)) {
                case ColumnType.DECIMAL8 ->
                        new UnscaledWidenDecimal8UncheckedFuncAbstract(valuePosition, targetType, value);
                case ColumnType.DECIMAL16 ->
                        new UnscaledWidenDecimal16UncheckedFuncAbstract(valuePosition, targetType, value);
                case ColumnType.DECIMAL32 ->
                        new UnscaledWidenDecimal32UncheckedFuncAbstract(valuePosition, targetType, value);
                case ColumnType.DECIMAL64 ->
                        new UnscaledWidenDecimal64UncheckedFuncAbstract(valuePosition, targetType, value);
                case ColumnType.DECIMAL128 ->
                        new UnscaledWidenDecimal128UncheckedFuncAbstract(valuePosition, targetType, value);
                default -> new UnscaledWidenDecimal256UncheckedFuncAbstract(valuePosition, targetType, value);
            };
        }

        return switch (ColumnType.tagOf(fromType)) {
            case ColumnType.DECIMAL8 -> new UnscaledNarrowDecimal8FuncAbstract(valuePosition, targetType, value);
            case ColumnType.DECIMAL16 -> new UnscaledNarrowDecimal16FuncAbstract(valuePosition, targetType, value);
            case ColumnType.DECIMAL32 -> new UnscaledNarrowDecimal32FuncAbstract(valuePosition, targetType, value);
            case ColumnType.DECIMAL64 -> new UnscaledNarrowDecimal64FuncAbstract(valuePosition, targetType, value);
            case ColumnType.DECIMAL128 -> new UnscaledNarrowDecimal128FuncAbstract(valuePosition, targetType, value);
            default -> new UnscaledNarrowDecimal256FuncAbstract(valuePosition, targetType, value);
        };
    }

    private static class ScaledDecimal128FuncAbstract extends ScaledDecimalFunctionAbstract {
        private final Decimal128 decimal128 = new Decimal128();

        public ScaledDecimal128FuncAbstract(int position, int targetType, Function value, int fromScale) {
            super(value, targetType, position, fromScale);
        }

        protected boolean load(Record rec) {
            arg.getDecimal128(rec, decimal128);
            if (decimal128.isNull()) {
                return false;
            }
            decimal.of(decimal128, fromScale);
            return true;
        }
    }

    private static class ScaledDecimal16FuncAbstract extends ScaledDecimalFunctionAbstract {
        public ScaledDecimal16FuncAbstract(int position, int targetType, Function value, int fromScale) {
            super(value, targetType, position, fromScale);
        }

        protected boolean load(Record rec) {
            short value = this.arg.getDecimal16(rec);
            if (value == Decimals.DECIMAL16_NULL) {
                return false;
            }
            decimal.ofLong(value, fromScale);
            return true;
        }
    }

    private static class ScaledDecimal256FuncAbstract extends ScaledDecimalFunctionAbstract {
        public ScaledDecimal256FuncAbstract(int position, int targetType, Function value, int fromScale) {
            super(value, targetType, position, fromScale);
        }

        protected boolean load(Record rec) {
            this.arg.getDecimal256(rec, decimal);
            if (decimal.isNull()) {
                return false;
            }
            decimal.setScale(fromScale);
            return true;
        }
    }

    private static class ScaledDecimal32FuncAbstract extends ScaledDecimalFunctionAbstract {
        public ScaledDecimal32FuncAbstract(int position, int targetType, Function value, int fromScale) {
            super(value, targetType, position, fromScale);
        }

        protected boolean load(Record rec) {
            int value = this.arg.getDecimal32(rec);
            if (value == Decimals.DECIMAL32_NULL) {
                return false;
            }
            decimal.ofLong(value, fromScale);
            return true;
        }
    }

    private static class ScaledDecimal64FuncAbstract extends ScaledDecimalFunctionAbstract {
        public ScaledDecimal64FuncAbstract(int position, int targetType, Function value, int fromScale) {
            super(value, targetType, position, fromScale);
        }

        protected boolean load(Record rec) {
            long value = this.arg.getDecimal64(rec);
            if (value == Decimals.DECIMAL64_NULL) {
                return false;
            }
            decimal.ofLong(value, fromScale);
            return true;
        }
    }

    private static class ScaledDecimal8FuncAbstract extends ScaledDecimalFunctionAbstract {
        public ScaledDecimal8FuncAbstract(int position, int targetType, Function value, int fromScale) {
            super(value, targetType, position, fromScale);
        }

        protected boolean load(Record rec) {
            byte value = this.arg.getDecimal8(rec);
            if (value == Decimals.DECIMAL8_NULL) {
                return false;
            }
            decimal.ofLong(value, fromScale);
            return true;
        }
    }

    private abstract static class ScaledDecimalFunctionAbstract extends AbstractCastToDecimalFunction {
        protected final int fromScale;

        public ScaledDecimalFunctionAbstract(Function arg, int targetType, int position, int fromScale) {
            super(arg, targetType, position);
            this.fromScale = fromScale;
        }

        protected boolean cast(Record rec) {
            if (!load(rec)) {
                return false;
            }
            long hh = decimal.getHh();
            long hl = decimal.getHl();
            long lh = decimal.getLh();
            long ll = decimal.getLl();
            try {
                decimal.rescale(scale);
            } catch (NumericException ignored) {
                decimal.of(hh, hl, lh, ll, fromScale);
                throw ImplicitCastException.inconvertibleValue(decimal, arg.getType(), type).position(position);
            }
            if (!decimal.comparePrecision(precision)) {
                decimal.of(hh, hl, lh, ll, fromScale);
                throw ImplicitCastException.inconvertibleValue(decimal, arg.getType(), type).position(position);
            }
            return true;
        }

        protected abstract boolean load(Record rec);
    }

    private static class UnscaledNarrowDecimal128FuncAbstract extends AbstractCastToDecimalFunction {
        private final Decimal128 decimal128 = new Decimal128();

        public UnscaledNarrowDecimal128FuncAbstract(int position, int targetType, Function value) {
            super(value, targetType, position);
        }

        protected boolean cast(Record rec) {
            this.arg.getDecimal128(rec, decimal128);
            if (decimal128.isNull()) {
                return false;
            }
            decimal.of(decimal128, 0);
            if (!decimal.comparePrecision(precision)) {
                throw ImplicitCastException.inconvertibleValue(decimal, arg.getType(), type).position(position);
            }
            return true;
        }
    }

    private static class UnscaledNarrowDecimal16FuncAbstract extends AbstractCastToDecimalFunction {
        final short maxValue;
        final short minValue;

        public UnscaledNarrowDecimal16FuncAbstract(int position, int targetType, Function value) {
            super(value, targetType, position);
            maxValue = (short) Math.min(Numbers.getMaxValue(precision), Short.MAX_VALUE);
            minValue = (short) (maxValue == Short.MAX_VALUE ? Short.MIN_VALUE : -maxValue);
        }

        protected boolean cast(Record rec) {
            short value = this.arg.getDecimal16(rec);
            if (value == Decimals.DECIMAL16_NULL) {
                return false;
            }
            if (value < minValue || value > maxValue) {
                throw ImplicitCastException.inconvertibleValue(value, arg.getType(), type).position(position);
            }
            decimal.ofLong(value, 0);
            return true;
        }
    }

    private static class UnscaledNarrowDecimal256FuncAbstract extends AbstractCastToDecimalFunction {
        public UnscaledNarrowDecimal256FuncAbstract(int position, int targetType, Function value) {
            super(value, targetType, position);
        }

        protected boolean cast(Record rec) {
            this.arg.getDecimal256(rec, decimal);
            if (decimal.isNull()) {
                return false;
            }
            decimal.setScale(0);
            if (!decimal.comparePrecision(precision)) {
                throw ImplicitCastException.inconvertibleValue(decimal, arg.getType(), type).position(position);
            }
            return true;
        }
    }

    private static class UnscaledNarrowDecimal32FuncAbstract extends AbstractCastToDecimalFunction {
        final int maxValue;
        final int minValue;

        public UnscaledNarrowDecimal32FuncAbstract(int position, int targetType, Function value) {
            super(value, targetType, position);
            maxValue = (int) Math.min(Numbers.getMaxValue(precision), Integer.MAX_VALUE);
            minValue = -maxValue;
        }

        protected boolean cast(Record rec) {
            int value = this.arg.getDecimal32(rec);
            if (value == Decimals.DECIMAL32_NULL) {
                return false;
            }
            if (value < minValue || value > maxValue) {
                throw ImplicitCastException.inconvertibleValue(value, arg.getType(), type).position(position);
            }
            decimal.ofLong(value, 0);
            return true;
        }
    }

    private static class UnscaledNarrowDecimal64FuncAbstract extends AbstractCastToDecimalFunction {
        final long maxValue;
        final long minValue;

        public UnscaledNarrowDecimal64FuncAbstract(int position, int targetType, Function value) {
            super(value, targetType, position);
            maxValue = Numbers.getMaxValue(precision);
            minValue = -maxValue;
        }

        protected boolean cast(Record rec) {
            long value = this.arg.getDecimal64(rec);
            if (value == Decimals.DECIMAL64_NULL) {
                return false;
            }
            if (value < minValue || value > maxValue) {
                throw ImplicitCastException.inconvertibleValue(value, arg.getType(), type).position(position);
            }
            decimal.ofLong(value, 0);
            return true;
        }
    }

    private static class UnscaledNarrowDecimal8FuncAbstract extends AbstractCastToDecimalFunction {
        final byte maxValue;
        final byte minValue;

        public UnscaledNarrowDecimal8FuncAbstract(int position, int targetType, Function value) {
            super(value, targetType, position);
            maxValue = (byte) Math.min(Numbers.getMaxValue(precision), Byte.MAX_VALUE);
            minValue = (byte) (maxValue == Byte.MAX_VALUE ? Byte.MIN_VALUE : -maxValue);
        }

        protected boolean cast(Record rec) {
            byte value = this.arg.getDecimal8(rec);
            if (value == Decimals.DECIMAL8_NULL) {
                return false;
            }
            if (value < minValue || value > maxValue) {
                throw ImplicitCastException.inconvertibleValue(value, arg.getType(), type).position(position);
            }
            decimal.ofLong(value, 0);
            return true;
        }
    }

    private static class UnscaledWidenDecimal128UncheckedFuncAbstract extends AbstractCastToDecimalFunction {
        private final Decimal128 decimal128 = new Decimal128();

        public UnscaledWidenDecimal128UncheckedFuncAbstract(int position, int targetType, Function value) {
            super(value, targetType, position);
        }

        protected boolean cast(Record rec) {
            arg.getDecimal128(rec, decimal128);
            if (decimal128.isNull()) {
                return false;
            }
            decimal.of(decimal128, 0);
            return true;
        }
    }

    private static class UnscaledWidenDecimal16UncheckedFuncAbstract extends AbstractCastToDecimalFunction {
        public UnscaledWidenDecimal16UncheckedFuncAbstract(int position, int targetType, Function value) {
            super(value, targetType, position);
        }

        protected boolean cast(Record rec) {
            short value = this.arg.getDecimal16(rec);
            if (value == Decimals.DECIMAL16_NULL) {
                return false;
            }
            decimal.ofLong(value, 0);
            return true;
        }
    }

    private static class UnscaledWidenDecimal256UncheckedFuncAbstract extends AbstractCastToDecimalFunction {
        public UnscaledWidenDecimal256UncheckedFuncAbstract(int position, int targetType, Function value) {
            super(value, targetType, position);
        }

        protected boolean cast(Record rec) {
            this.arg.getDecimal256(rec, decimal);
            if (decimal.isNull()) {
                return false;
            }
            decimal.setScale(0);
            return true;
        }
    }

    private static class UnscaledWidenDecimal32UncheckedFuncAbstract extends AbstractCastToDecimalFunction {
        public UnscaledWidenDecimal32UncheckedFuncAbstract(int position, int targetType, Function value) {
            super(value, targetType, position);
        }

        protected boolean cast(Record rec) {
            int value = this.arg.getDecimal32(rec);
            if (value == Decimals.DECIMAL32_NULL) {
                return false;
            }
            decimal.ofLong(value, 0);
            return true;
        }
    }

    private static class UnscaledWidenDecimal64UncheckedFuncAbstract extends AbstractCastToDecimalFunction {
        public UnscaledWidenDecimal64UncheckedFuncAbstract(int position, int targetType, Function value) {
            super(value, targetType, position);
        }

        protected boolean cast(Record rec) {
            long value = this.arg.getDecimal64(rec);
            if (value == Decimals.DECIMAL64_NULL) {
                return false;
            }
            decimal.ofLong(value, 0);
            return true;
        }
    }

    private static class UnscaledWidenDecimal8UncheckedFuncAbstract extends AbstractCastToDecimalFunction {
        public UnscaledWidenDecimal8UncheckedFuncAbstract(int position, int targetType, Function value) {
            super(value, targetType, position);
        }

        protected boolean cast(Record rec) {
            byte value = this.arg.getDecimal8(rec);
            if (value == Decimals.DECIMAL8_NULL) {
                return false;
            }
            decimal.ofLong(value, 0);
            return true;
        }
    }
}
