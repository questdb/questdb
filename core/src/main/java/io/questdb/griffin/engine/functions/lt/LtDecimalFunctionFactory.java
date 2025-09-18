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

package io.questdb.griffin.engine.functions.lt;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class LtDecimalFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "<(ΞΞ)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function left = args.getQuick(0);
        Function right = args.getQuick(1);
        left = DecimalUtil.maybeRescaleDecimalConstant(
                left,
                sqlExecutionContext.getDecimal256(),
                ColumnType.getDecimalPrecision(right.getType()),
                ColumnType.getDecimalScale(right.getType())
        );
        args.setQuick(0, left); // the OG function may be already closed
        right = DecimalUtil.maybeRescaleDecimalConstant(
                right,
                sqlExecutionContext.getDecimal256(),
                ColumnType.getDecimalPrecision(left.getType()),
                ColumnType.getDecimalScale(left.getType())
        );
        args.setQuick(1, right);

        final int leftType = left.getType();
        final int rightType = right.getType();
        final int leftPrecision = ColumnType.getDecimalPrecision(leftType);
        final int rightPrecision = ColumnType.getDecimalPrecision(rightType);
        final int leftScale = ColumnType.getDecimalScale(leftType);
        final int rightScale = ColumnType.getDecimalScale(rightType);

        if (leftPrecision == rightPrecision && leftScale == rightScale) {
            switch (Decimals.getStorageSizePow2(leftPrecision)) {
                case 0:
                    return new UnscaledDecimal8Func(left, right);
                case 1:
                    return new UnscaledDecimal16Func(left, right);
                case 2:
                    return new UnscaledDecimal32Func(left, right);
                case 3:
                    return new UnscaledDecimal64Func(left, right);
                case 4:
                    return new UnscaledDecimal128Func(left, right);
                default:
                    return new UnscaledDecimal256Func(left, right);
            }
        }

        final int maxPrecision = Math.max(leftPrecision, rightPrecision);
        switch (Decimals.getStorageSizePow2(maxPrecision)) {
            case 0:
            case 1:
            case 2:
            case 3:
                return new Decimal64Func(left, right);
            case 4:
                return new Decimal128Func(left, right);
            default:
                return new Decimal256Func(left, right);
        }
    }

    private static class Decimal128Func extends CompareDecimal128Function {

        public Decimal128Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(left);
            if (negated) {
                sink.val(">=");
            } else {
                sink.val('<');
            }
            sink.val(right);
        }

        @Override
        protected boolean exec(long rightHigh, long rightLow, int rightScale) {
            return decimal.compareTo(rightHigh, rightLow, rightScale) < 0;
        }
    }

    private static class Decimal256Func extends CompareDecimal256Function {

        public Decimal256Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(left);
            if (negated) {
                sink.val(">=");
            } else {
                sink.val('<');
            }
            sink.val(right);
        }

        @Override
        protected boolean exec(long rightHH, long rightHL, long rightLH, long rightLL, int rightScale) {
            return decimal.compareTo(rightHH, rightHL, rightLH, rightLL, rightScale) < 0;
        }
    }

    private static class Decimal64Func extends CompareDecimal64Function {

        public Decimal64Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(left);
            if (negated) {
                sink.val(">=");
            } else {
                sink.val('<');
            }
            sink.val(right);
        }

        @Override
        protected boolean exec(long rightValue, int rightScale) {
            return decimal.compareTo(rightValue, rightScale) < 0;
        }
    }

    private static class UnscaledDecimal128Func extends AbstractLtBinaryFunction {

        public UnscaledDecimal128Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            final long aHi = left.getDecimal128Hi(rec);
            final long aLo = left.getDecimal128Lo(rec);
            final long bHi = right.getDecimal128Hi(rec);
            final long bLo = right.getDecimal128Lo(rec);

            if (Decimal128.isNull(aHi, aLo) || Decimal128.isNull(bHi, bLo)) {
                return false;
            }
            if (aHi != bHi) {
                return negated == (aHi > bHi);
            }
            return negated == (Long.compareUnsigned(aLo, bLo) >= 0);
        }
    }

    private static class UnscaledDecimal16Func extends AbstractLtBinaryFunction {

        public UnscaledDecimal16Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            final long a = left.getDecimal16(rec);
            final long b = right.getDecimal16(rec);
            if (a == Decimals.DECIMAL16_NULL || b == Decimals.DECIMAL16_NULL) {
                return false;
            }
            return negated == (a >= b);
        }
    }

    private static class UnscaledDecimal256Func extends AbstractLtBinaryFunction {

        public UnscaledDecimal256Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            final long aHH = left.getDecimal256HH(rec);
            final long aHL = left.getDecimal256HL(rec);
            final long aLH = left.getDecimal256LH(rec);
            final long aLL = left.getDecimal256LL(rec);
            final long bHH = right.getDecimal256HH(rec);
            final long bHL = right.getDecimal256HL(rec);
            final long bLH = right.getDecimal256LH(rec);
            final long bLL = right.getDecimal256LL(rec);

            if (Decimal256.isNull(aHH, aHL, aLH, aLL) || Decimal256.isNull(bHH, bHL, bLH, bLL)) {
                return false;
            }
            if (aHH != bHH) {
                return negated == (aHH > bHH);
            }
            if (aHL != bHL) {
                return negated == (Long.compareUnsigned(aHL, bHL) > 0);
            }
            if (aLH != bLH) {
                return negated == (Long.compareUnsigned(aLH, bLH) > 0);
            }
            return negated == (Long.compareUnsigned(aLL, bLL) >= 0);
        }
    }

    private static class UnscaledDecimal32Func extends AbstractLtBinaryFunction {

        public UnscaledDecimal32Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            final long a = left.getDecimal32(rec);
            final long b = right.getDecimal32(rec);
            if (a == Decimals.DECIMAL32_NULL || b == Decimals.DECIMAL32_NULL) {
                return false;
            }
            return negated == (a >= b);
        }
    }

    private static class UnscaledDecimal64Func extends AbstractLtBinaryFunction {

        public UnscaledDecimal64Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            final long a = left.getDecimal64(rec);
            final long b = right.getDecimal64(rec);
            if (a == Decimals.DECIMAL64_NULL || b == Decimals.DECIMAL64_NULL) {
                return false;
            }
            return negated == (a >= b);
        }
    }

    private static class UnscaledDecimal8Func extends AbstractLtBinaryFunction {

        public UnscaledDecimal8Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            final long a = left.getDecimal8(rec);
            final long b = right.getDecimal8(rec);
            if (a == Decimals.DECIMAL8_NULL || b == Decimals.DECIMAL8_NULL) {
                return false;
            }
            return negated == (a >= b);
        }
    }
}
