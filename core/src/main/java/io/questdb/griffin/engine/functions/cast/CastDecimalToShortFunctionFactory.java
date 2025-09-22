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
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

import java.math.RoundingMode;

public class CastDecimalToShortFunctionFactory implements FunctionFactory {

    public static Function newInstance(
            int position,
            Function arg,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (arg.isConstant()) {
            return newConstantInstance(sqlExecutionContext, position, arg);
        }

        final int fromType = arg.getType();
        final int fromScale = ColumnType.getDecimalScale(fromType);
        if (fromScale == 0) {
            return newUnscaledInstance(position, fromType, arg);
        }

        return new ScaledDecimalFunction(arg, position);
    }

    @Override
    public String getSignature() {
        return "cast(Ξe)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return newInstance(position, args.getQuick(0), sqlExecutionContext);
    }

    private static Function newConstantInstance(
            SqlExecutionContext sqlExecutionContext,
            int valuePosition,
            Function value
    ) {
        final int fromType = value.getType();
        final int fromScale = ColumnType.getDecimalScale(fromType);

        Decimal256 decimal = sqlExecutionContext.getDecimal256();
        DecimalUtil.load(decimal, value, null, fromType);

        if (decimal.isNull()) {
            return ShortConstant.ZERO;
        }

        long hh = decimal.getHh();
        long hl = decimal.getHl();
        long lh = decimal.getLh();
        long ll = decimal.getLl();
        if (fromScale != 0) {
            decimal.round(0, RoundingMode.DOWN);
        }

        // Ensures that the value fits in a short
        if (overflowsShort(decimal)) {
            decimal.of(hh, hl, lh, ll, fromScale);
            throw ImplicitCastException.inconvertibleValue(decimal, fromType, ColumnType.SHORT).position(valuePosition);
        }

        return ShortConstant.newInstance((short) decimal.getLl());
    }

    private static Function newUnscaledInstance(int valuePosition, int fromType, Function value) {
        int fromPrecision = ColumnType.getDecimalPrecision(fromType);
        switch (Decimals.getStorageSizePow2(fromPrecision)) {
            case 0:
                return new UnscaledDecimal8Func(value);
            case 1:
                return new UnscaledDecimal16Func(value);
            case 2:
                return new UnscaledDecimal32Func(value, valuePosition);
            case 3:
                return new UnscaledDecimal64Func(value, valuePosition);
            case 4:
                return new UnscaledDecimal128Func(value, valuePosition);
            default:
                return new UnscaledDecimal256Func(value, valuePosition);
        }
    }

    private static boolean overflowsShort(Decimal256 decimal) {
        return decimal.compareTo(0, 0, 0, Short.MAX_VALUE, 0) > 0 ||
                decimal.compareTo(-1, -1, -1, Short.MIN_VALUE, 0) < 0;
    }

    private static class ScaledDecimalFunction extends AbstractCastToShortFunction {
        private final Decimal256 decimal256 = new Decimal256();
        private final int fromScale;
        private final int fromType;
        private final int position;

        public ScaledDecimalFunction(Function arg, int position) {
            super(arg);
            this.fromType = arg.getType();
            this.position = position;
            this.fromScale = ColumnType.getDecimalScale(fromType);
        }

        public short getShort(Record rec) {
            DecimalUtil.load(decimal256, arg, rec, fromType);
            if (decimal256.isNull()) {
                return Numbers.SHORT_NULL;
            }
            long hh = decimal256.getHh();
            long hl = decimal256.getHl();
            long lh = decimal256.getLh();
            long ll = decimal256.getLl();
            decimal256.round(0, RoundingMode.DOWN);
            if (overflowsShort(decimal256)) {
                decimal256.of(hh, hl, lh, ll, fromScale);
                throw ImplicitCastException.inconvertibleValue(decimal256, arg.getType(), ColumnType.SHORT).position(position);
            }
            return (short) decimal256.getLl();
        }
    }

    private static class UnscaledDecimal128Func extends AbstractCastToShortFunction {
        private final Decimal256 decimal256 = new Decimal256();
        private final int position;

        public UnscaledDecimal128Func(Function value, int position) {
            super(value);
            this.position = position;
        }

        public short getShort(Record rec) {
            long hi = this.arg.getDecimal128Hi(rec);
            long lo = this.arg.getDecimal128Lo(rec);
            if (Decimal128.isNull(hi, lo)) {
                return Numbers.SHORT_NULL;
            }
            long s = hi < 0 ? -1 : 0;
            decimal256.of(s, s, hi, lo, 0);
            if (overflowsShort(decimal256)) {
                throw ImplicitCastException.inconvertibleValue(decimal256, arg.getType(), ColumnType.SHORT).position(position);
            }
            return (short) lo;
        }
    }

    private static class UnscaledDecimal16Func extends AbstractCastToShortFunction {
        public UnscaledDecimal16Func(Function value) {
            super(value);
        }

        public short getShort(Record rec) {
            short value = this.arg.getDecimal16(rec);
            if (value == Decimals.DECIMAL16_NULL) {
                return Numbers.SHORT_NULL;
            }
            return value;
        }
    }

    private static class UnscaledDecimal256Func extends AbstractCastToShortFunction {
        private final Decimal256 decimal256 = new Decimal256();
        private final int position;

        public UnscaledDecimal256Func(Function value, int position) {
            super(value);
            this.position = position;
        }

        public short getShort(Record rec) {
            long hh = this.arg.getDecimal256HH(rec);
            long hl = this.arg.getDecimal256HL(rec);
            long lh = this.arg.getDecimal256LH(rec);
            long ll = this.arg.getDecimal256LL(rec);
            decimal256.of(hh, hl, lh, ll, 0);
            if (decimal256.isNull()) {
                return Numbers.SHORT_NULL;
            }
            if (overflowsShort(decimal256)) {
                throw ImplicitCastException.inconvertibleValue(decimal256, arg.getType(), ColumnType.SHORT).position(position);
            }
            return (short) ll;
        }
    }

    private static class UnscaledDecimal32Func extends AbstractCastToShortFunction {
        private final int position;

        public UnscaledDecimal32Func(Function value, int position) {
            super(value);
            this.position = position;
        }

        public short getShort(Record rec) {
            int value = this.arg.getDecimal32(rec);
            if (value == Decimals.DECIMAL32_NULL) {
                return Numbers.SHORT_NULL;
            }
            if (value > Short.MAX_VALUE || value < Short.MIN_VALUE) {
                throw ImplicitCastException.inconvertibleValue(value, arg.getType(), ColumnType.SHORT).position(position);
            }
            return (short) value;
        }
    }

    private static class UnscaledDecimal64Func extends AbstractCastToShortFunction {
        private final int position;

        public UnscaledDecimal64Func(Function value, int position) {
            super(value);
            this.position = position;
        }

        public short getShort(Record rec) {
            long value = this.arg.getDecimal64(rec);
            if (value == Decimals.DECIMAL64_NULL) {
                return Numbers.SHORT_NULL;
            }
            if (value > Short.MAX_VALUE || value < Short.MIN_VALUE) {
                throw ImplicitCastException.inconvertibleValue(value, arg.getType(), ColumnType.SHORT).position(position);
            }
            return (short) value;
        }
    }

    private static class UnscaledDecimal8Func extends AbstractCastToShortFunction {
        public UnscaledDecimal8Func(Function value) {
            super(value);
        }

        public short getShort(Record rec) {
            byte value = this.arg.getDecimal8(rec);
            if (value == Decimals.DECIMAL8_NULL) {
                return Numbers.SHORT_NULL;
            }
            return value;
        }
    }
}
