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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class EqDecimalFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "=(ΞΞ)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function left = args.getQuick(0);
        final Function right = args.getQuick(1);
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

    private static class Decimal128Func extends AbstractEqBinaryFunction {
        private final Decimal128 decimal = new Decimal128();
        private final int rightScale;
        private final int rightStorageSizePow2;

        public Decimal128Func(Function left, Function right) {
            super(left, right);
            this.rightScale = ColumnType.getDecimalScale(right.getType());
            this.rightStorageSizePow2 = Decimals.getStorageSizePow2(ColumnType.getDecimalPrecision(right.getType()));
        }

        @Override
        public boolean getBool(Record rec) {
            DecimalUtil.load(decimal, left, rec);
            final long rightHigh, rightLow;
            switch (rightStorageSizePow2) {
                case 0:
                    rightLow = right.getDecimal8(rec);
                    rightHigh = rightLow < 0 ? -1L : 0L;
                    break;
                case 1:
                    rightLow = right.getDecimal16(rec);
                    rightHigh = rightLow < 0 ? -1L : 0L;
                    break;
                case 2:
                    rightLow = right.getDecimal32(rec);
                    rightHigh = rightLow < 0 ? -1L : 0L;
                    break;
                case 3:
                    rightLow = right.getDecimal64(rec);
                    rightHigh = rightLow < 0 ? -1L : 0L;
                    break;
                default:
                    rightHigh = right.getDecimal128Hi(rec);
                    rightLow = right.getDecimal128Lo(rec);
                    break;
            }
            return negated != (decimal.compareTo(rightHigh, rightLow, rightScale) == 0);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class Decimal256Func extends AbstractEqBinaryFunction {
        private final Decimal256 decimal = new Decimal256();
        private final int rightScale;
        private final int rightStorageSizePow2;

        public Decimal256Func(Function left, Function right) {
            super(left, right);
            this.rightScale = ColumnType.getDecimalScale(right.getType());
            this.rightStorageSizePow2 = Decimals.getStorageSizePow2(ColumnType.getDecimalPrecision(right.getType()));
        }

        @Override
        public boolean getBool(Record rec) {
            DecimalUtil.load(decimal, left, rec);
            final long rightHH, rightHL, rightLH, rightLL;
            switch (rightStorageSizePow2) {
                case 0:
                    rightLL = right.getDecimal8(rec);
                    rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                    break;
                case 1:
                    rightLL = right.getDecimal16(rec);
                    rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                    break;
                case 2:
                    rightLL = right.getDecimal32(rec);
                    rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                    break;
                case 3:
                    rightLL = right.getDecimal64(rec);
                    rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                    break;
                case 4:
                    rightLH = right.getDecimal128Hi(rec);
                    rightLL = right.getDecimal128Lo(rec);
                    rightHL = rightHH = rightLH < 0 ? -1 : 0;
                    break;
                default:
                    rightHH = right.getDecimal256HH(rec);
                    rightHL = right.getDecimal256HL(rec);
                    rightLH = right.getDecimal256LH(rec);
                    rightLL = right.getDecimal256LL(rec);
                    break;
            }
            return negated != (decimal.compareTo(rightHH, rightHL, rightLH, rightLL, rightScale) == 0);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class Decimal64Func extends AbstractEqBinaryFunction {
        private final Decimal64 decimal = new Decimal64();
        private final int rightScale;
        private final int rightStorageSizePow2;

        public Decimal64Func(Function left, Function right) {
            super(left, right);
            this.rightScale = ColumnType.getDecimalScale(right.getType());
            this.rightStorageSizePow2 = Decimals.getStorageSizePow2(ColumnType.getDecimalPrecision(right.getType()));
        }

        @Override
        public boolean getBool(Record rec) {
            DecimalUtil.load(decimal, left, rec);
            final long rightValue;
            switch (rightStorageSizePow2) {
                case 0:
                    rightValue = right.getDecimal8(rec);
                    break;
                case 1:
                    rightValue = right.getDecimal16(rec);
                    break;
                case 2:
                    rightValue = right.getDecimal32(rec);
                    break;
                default:
                    rightValue = right.getDecimal64(rec);
                    break;
            }
            return negated != (decimal.compareTo(rightValue, rightScale) == 0);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class UnscaledDecimal128Func extends AbstractEqBinaryFunction {

        public UnscaledDecimal128Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (
                    left.getDecimal128Hi(rec) == right.getDecimal128Hi(rec)
                            && left.getDecimal128Lo(rec) == right.getDecimal128Lo(rec)
            );
        }
    }

    private static class UnscaledDecimal16Func extends AbstractEqBinaryFunction {

        public UnscaledDecimal16Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (left.getDecimal16(rec) == right.getDecimal16(rec));
        }
    }

    private static class UnscaledDecimal256Func extends AbstractEqBinaryFunction {

        public UnscaledDecimal256Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (
                    left.getDecimal256HH(rec) == right.getDecimal256HH(rec)
                            && left.getDecimal256HL(rec) == right.getDecimal256HL(rec)
                            && left.getDecimal256LH(rec) == right.getDecimal256LH(rec)
                            && left.getDecimal256LL(rec) == right.getDecimal256LL(rec)
            );
        }
    }

    private static class UnscaledDecimal32Func extends AbstractEqBinaryFunction {

        public UnscaledDecimal32Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (left.getDecimal32(rec) == right.getDecimal32(rec));
        }
    }

    private static class UnscaledDecimal64Func extends AbstractEqBinaryFunction {

        public UnscaledDecimal64Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (left.getDecimal64(rec) == right.getDecimal64(rec));
        }
    }

    private static class UnscaledDecimal8Func extends AbstractEqBinaryFunction {

        public UnscaledDecimal8Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (left.getDecimal8(rec) == right.getDecimal8(rec));
        }
    }
}
