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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.lt.CompareDecimal128Function;
import io.questdb.griffin.engine.functions.lt.CompareDecimal256Function;
import io.questdb.griffin.engine.functions.lt.CompareDecimal64Function;
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
                sink.val('!');
            }
            sink.val('=').val(right);
        }

        @Override
        protected boolean exec(long rightHigh, long rightLow, int rightScale) {
            return decimal.compareTo(rightHigh, rightLow, rightScale) == 0;
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
                sink.val('!');
            }
            sink.val('=').val(right);
        }

        @Override
        protected boolean exec(long rightHH, long rightHL, long rightLH, long rightLL, int rightScale) {
            return decimal.compareTo(rightHH, rightHL, rightLH, rightLL, rightScale) == 0;
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
                sink.val('!');
            }
            sink.val('=').val(right);
        }

        @Override
        protected boolean exec(long rightValue, int rightScale) {
            return decimal.compareTo(rightValue, rightScale) == 0;
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
