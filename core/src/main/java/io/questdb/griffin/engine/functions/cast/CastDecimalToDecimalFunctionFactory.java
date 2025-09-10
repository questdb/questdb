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
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

public class CastDecimalToDecimalFunctionFactory implements FunctionFactory {
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
        final Function arg = args.getQuick(0);
        final int targetType = args.getQuick(1).getType();
        if (arg.isConstant()) {
            return newConstantInstance(sqlExecutionContext, argPositions.get(0), targetType, arg);
        }

        final int fromType = arg.getType();
        final int fromScale = ColumnType.getDecimalScale(fromType);
        final int targetScale = ColumnType.getDecimalScale(targetType);
        if (fromScale == targetScale) {
            return newUnscaledInstance(argPositions.get(0), fromType, targetType, arg);
        }

        final int fromPrecision = ColumnType.getDecimalPrecision(fromType);
        switch (Decimals.getStorageSizePow2(fromPrecision)) {
            case 0:
                return new ScaledDecimal8Func(argPositions.get(0), targetType, arg, fromScale);
            case 1:
                return new ScaledDecimal16Func(argPositions.get(0), targetType, arg, fromScale);
            case 2:
                return new ScaledDecimal32Func(argPositions.get(0), targetType, arg, fromScale);
            case 3:
                return new ScaledDecimal64Func(argPositions.get(0), targetType, arg, fromScale);
            case 4:
                return new ScaledDecimal128Func(argPositions.get(0), targetType, arg, fromScale);
            default:
                return new ScaledDecimal256Func(argPositions.get(0), targetType, arg, fromScale);
        }
    }

    private static Function newUnscaledInstance(int valuePosition, int fromType, int targetType, Function value) {
        int fromPrecision = ColumnType.getDecimalPrecision(fromType);
        int targetPrecision = ColumnType.getDecimalPrecision(targetType);
        if (targetPrecision >= fromPrecision) {
            switch (Decimals.getStorageSizePow2(fromPrecision)) {
                case 0:
                    return new UnscaledWidenDecimal8UncheckedFunc(valuePosition, targetType, value);
                case 1:
                    return new UnscaledWidenDecimal16UncheckedFunc(valuePosition, targetType, value);
                case 2:
                    return new UnscaledWidenDecimal32UncheckedFunc(valuePosition, targetType, value);
                case 3:
                    return new UnscaledWidenDecimal64UncheckedFunc(valuePosition, targetType, value);
                case 4:
                    return new UnscaledWidenDecimal128UncheckedFunc(valuePosition, targetType, value);
                default:
                    return new UnscaledWidenDecimal256UncheckedFunc(valuePosition, targetType, value);
            }
        }

        switch (Decimals.getStorageSizePow2(fromPrecision)) {
            case 0:
                return new UnscaledNarrowDecimal8Func(valuePosition, targetType, value);
            case 1:
                return new UnscaledNarrowDecimal16Func(valuePosition, targetType, value);
            case 2:
                return new UnscaledNarrowDecimal32Func(valuePosition, targetType, value);
            case 3:
                return new UnscaledNarrowDecimal64Func(valuePosition, targetType, value);
            case 4:
                return new UnscaledNarrowDecimal128Func(valuePosition, targetType, value);
            default:
                return new UnscaledNarrowDecimal256Func(valuePosition, targetType, value);
        }
    }

    private Function newConstantInstance(
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
        DecimalUtil.load(decimal, value, null);

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

    private static class ScaledDecimal128Func extends ScaledDecimalFunction {
        public ScaledDecimal128Func(int position, int targetType, Function value, int fromScale) {
            super(value, targetType, position, fromScale);
        }

        protected boolean load(Record rec) {
            long hi = this.arg.getDecimal128Hi(rec);
            long lo = this.arg.getDecimal128Lo(rec);
            if (Decimal128.isNull(hi, lo)) {
                return false;
            }
            long s = hi < 0 ? -1 : 0;
            decimal.of(s, s, hi, lo, fromScale);
            return true;
        }
    }

    private static class ScaledDecimal16Func extends ScaledDecimalFunction {
        public ScaledDecimal16Func(int position, int targetType, Function value, int fromScale) {
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

    private static class ScaledDecimal256Func extends ScaledDecimalFunction {
        public ScaledDecimal256Func(int position, int targetType, Function value, int fromScale) {
            super(value, targetType, position, fromScale);
        }

        protected boolean load(Record rec) {
            long hh = this.arg.getDecimal256HH(rec);
            long hl = this.arg.getDecimal256HL(rec);
            long lh = this.arg.getDecimal256LH(rec);
            long ll = this.arg.getDecimal256LL(rec);
            if (Decimal256.isNull(hh, hl, lh, ll)) {
                return false;
            }
            decimal.of(hh, hl, lh, ll, fromScale);
            return true;
        }
    }

    private static class ScaledDecimal32Func extends ScaledDecimalFunction {
        public ScaledDecimal32Func(int position, int targetType, Function value, int fromScale) {
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

    private static class ScaledDecimal64Func extends ScaledDecimalFunction {
        public ScaledDecimal64Func(int position, int targetType, Function value, int fromScale) {
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

    private static class ScaledDecimal8Func extends ScaledDecimalFunction {
        public ScaledDecimal8Func(int position, int targetType, Function value, int fromScale) {
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

    private abstract static class ScaledDecimalFunction extends AbstractCastToDecimalFunction {
        protected final int fromScale;

        public ScaledDecimalFunction(Function arg, int targetType, int position, int fromScale) {
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

    private static class UnscaledNarrowDecimal128Func extends AbstractCastToDecimalFunction {
        public UnscaledNarrowDecimal128Func(int position, int targetType, Function value) {
            super(value, targetType, position);
        }

        protected boolean cast(Record rec) {
            long hi = this.arg.getDecimal128Hi(rec);
            long lo = this.arg.getDecimal128Lo(rec);
            if (Decimal128.isNull(hi, lo)) {
                return false;
            }
            long s = hi < 0 ? -1 : 0;
            decimal.of(s, s, hi, lo, 0);
            if (!decimal.comparePrecision(precision)) {
                throw ImplicitCastException.inconvertibleValue(decimal, arg.getType(), type).position(position);
            }
            return true;
        }
    }

    private static class UnscaledNarrowDecimal16Func extends AbstractCastToDecimalFunction {
        final short maxValue;
        final short minValue;

        public UnscaledNarrowDecimal16Func(int position, int targetType, Function value) {
            super(value, targetType, position);
            maxValue = (short) Decimals.getMaxLong(precision);
            minValue = (short) -maxValue;
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

    private static class UnscaledNarrowDecimal256Func extends AbstractCastToDecimalFunction {
        public UnscaledNarrowDecimal256Func(int position, int targetType, Function value) {
            super(value, targetType, position);
        }

        protected boolean cast(Record rec) {
            long hh = this.arg.getDecimal256HH(rec);
            long hl = this.arg.getDecimal256HL(rec);
            long lh = this.arg.getDecimal256LH(rec);
            long ll = this.arg.getDecimal256LL(rec);
            if (Decimal256.isNull(hh, hl, lh, ll)) {
                return false;
            }
            decimal.of(hh, hl, lh, ll, 0);
            if (!decimal.comparePrecision(precision)) {
                throw ImplicitCastException.inconvertibleValue(decimal, arg.getType(), type).position(position);
            }
            return true;
        }
    }

    private static class UnscaledNarrowDecimal32Func extends AbstractCastToDecimalFunction {
        final int maxValue;
        final int minValue;

        public UnscaledNarrowDecimal32Func(int position, int targetType, Function value) {
            super(value, targetType, position);
            maxValue = (int) Decimals.getMaxLong(precision);
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

    private static class UnscaledNarrowDecimal64Func extends AbstractCastToDecimalFunction {
        final long maxValue;
        final long minValue;

        public UnscaledNarrowDecimal64Func(int position, int targetType, Function value) {
            super(value, targetType, position);
            maxValue = Decimals.getMaxLong(precision);
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

    private static class UnscaledNarrowDecimal8Func extends AbstractCastToDecimalFunction {
        final byte maxValue;
        final byte minValue;

        public UnscaledNarrowDecimal8Func(int position, int targetType, Function value) {
            super(value, targetType, position);
            maxValue = (byte) Decimals.getMaxLong(precision);
            minValue = (byte) -maxValue;
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

    private static class UnscaledWidenDecimal128UncheckedFunc extends AbstractCastToDecimalFunction {
        public UnscaledWidenDecimal128UncheckedFunc(int position, int targetType, Function value) {
            super(value, targetType, position);
        }

        protected boolean cast(Record rec) {
            long hi = this.arg.getDecimal128Hi(rec);
            long lo = this.arg.getDecimal128Lo(rec);
            if (Decimal128.isNull(hi, lo)) {
                return false;
            }
            long s = hi < 0 ? -1 : 0;
            decimal.of(s, s, hi, lo, 0);
            return true;
        }
    }

    private static class UnscaledWidenDecimal16UncheckedFunc extends AbstractCastToDecimalFunction {
        public UnscaledWidenDecimal16UncheckedFunc(int position, int targetType, Function value) {
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

    private static class UnscaledWidenDecimal256UncheckedFunc extends AbstractCastToDecimalFunction {
        public UnscaledWidenDecimal256UncheckedFunc(int position, int targetType, Function value) {
            super(value, targetType, position);
        }

        protected boolean cast(Record rec) {
            long hh = this.arg.getDecimal256HH(rec);
            long hl = this.arg.getDecimal256HL(rec);
            long lh = this.arg.getDecimal256LH(rec);
            long ll = this.arg.getDecimal256LL(rec);
            if (Decimal256.isNull(hh, hl, lh, ll)) {
                return false;
            }
            decimal.of(hh, hl, lh, ll, 0);
            return true;
        }
    }

    private static class UnscaledWidenDecimal32UncheckedFunc extends AbstractCastToDecimalFunction {
        public UnscaledWidenDecimal32UncheckedFunc(int position, int targetType, Function value) {
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

    private static class UnscaledWidenDecimal64UncheckedFunc extends AbstractCastToDecimalFunction {
        public UnscaledWidenDecimal64UncheckedFunc(int position, int targetType, Function value) {
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

    private static class UnscaledWidenDecimal8UncheckedFunc extends AbstractCastToDecimalFunction {
        public UnscaledWidenDecimal8UncheckedFunc(int position, int targetType, Function value) {
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
