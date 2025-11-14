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
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
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
                sqlExecutionContext.getDecimal128(),
                ColumnType.getDecimalPrecision(right.getType()),
                ColumnType.getDecimalScale(right.getType())
        );
        args.setQuick(0, left); // the OG function may be already closed
        right = DecimalUtil.maybeRescaleDecimalConstant(
                right,
                sqlExecutionContext.getDecimal256(),
                sqlExecutionContext.getDecimal128(),
                ColumnType.getDecimalPrecision(left.getType()),
                ColumnType.getDecimalScale(left.getType())
        );
        args.setQuick(1, right);

        final int leftType = left.getType();
        final int rightType = right.getType();
        final int leftTag = ColumnType.tagOf(leftType);
        final int rightTag = ColumnType.tagOf(rightType);
        final int leftScale = ColumnType.getDecimalScale(leftType);
        final int rightScale = ColumnType.getDecimalScale(rightType);

        if (leftTag == rightTag && leftScale == rightScale) {
            return switch (leftTag) {
                case ColumnType.DECIMAL8 -> new UnscaledDecimal8Func(left, right);
                case ColumnType.DECIMAL16 -> new UnscaledDecimal16Func(left, right);
                case ColumnType.DECIMAL32 -> new UnscaledDecimal32Func(left, right);
                case ColumnType.DECIMAL64 -> new UnscaledDecimal64Func(left, right);
                case ColumnType.DECIMAL128 -> new UnscaledDecimal128Func(left, right);
                default -> new UnscaledDecimal256Func(left, right);
            };
        }

        final int leftPrecision = ColumnType.getDecimalPrecision(leftType);
        final int rightPrecision = ColumnType.getDecimalPrecision(rightType);
        final int maxPrecision = Math.max(leftPrecision, rightPrecision);
        return switch (Decimals.getStorageSizePow2(maxPrecision)) {
            case 0, 1, 2, 3 -> new Decimal64Func(left, right);
            case 4 -> new Decimal128Func(left, right);
            default -> new Decimal256Func(left, right);
        };
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
        protected boolean exec() {
            return decimalLeft.compareTo(decimalRight) == 0;
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
        protected boolean exec() {
            return decimalLeft.compareTo(decimalRight) == 0;
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
        protected boolean exec() {
            return decimalLeft.compareTo(decimalRight) == 0;
        }
    }

    private static class UnscaledDecimal128Func extends AbstractEqBinaryFunction {
        private final Decimal128 decimalLeft = new Decimal128();
        private final Decimal128 decimalRight = new Decimal128();

        public UnscaledDecimal128Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            left.getDecimal128(rec, decimalLeft);
            right.getDecimal128(rec, decimalRight);
            return negated != (
                    decimalLeft.getHigh() == decimalRight.getHigh()
                            && decimalLeft.getLow() == decimalRight.getLow()
            );
        }

        @Override
        public boolean isThreadSafe() {
            return false;
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
        private final Decimal256 decimalLeft = new Decimal256();
        private final Decimal256 decimalRight = new Decimal256();

        public UnscaledDecimal256Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            left.getDecimal256(rec, decimalLeft);
            right.getDecimal256(rec, decimalRight);
            return negated != (
                    decimalLeft.getHh() == decimalRight.getHh()
                            && decimalLeft.getHl() == decimalRight.getHl()
                            && decimalLeft.getLh() == decimalRight.getLh()
                            && decimalLeft.getLl() == decimalRight.getLl()
            );
        }

        @Override
        public boolean isThreadSafe() {
            return false;
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
