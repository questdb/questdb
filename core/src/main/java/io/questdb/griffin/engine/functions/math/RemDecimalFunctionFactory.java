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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class RemDecimalFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "%(ΞΞ)";
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
        final int leftScale = ColumnType.getDecimalScale(leftType);
        final int rightScale = ColumnType.getDecimalScale(rightType);
        final int scale = Math.max(leftScale, rightScale);
        final int precision = Math.min(
                Math.max(ColumnType.getDecimalPrecision(leftType) - leftScale, ColumnType.getDecimalPrecision(rightType) - rightScale) + scale,
                Decimals.MAX_PRECISION
        );
        final int type = ColumnType.getDecimalType(precision, scale);

        return switch (ColumnType.tagOf(type)) {
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64 ->
                    new Decimal64Func(left, right, type, position);
            case ColumnType.DECIMAL128 -> new Decimal128Func(left, right, type, position);
            default -> new Decimal256Func(left, right, type, position);
        };
    }

    private static class Decimal128Func extends ArithmeticDecimal128Function {

        public Decimal128Func(Function left, Function right, int targetType, int position) {
            super(left, right, targetType, position);
        }

        @Override
        public String getName() {
            return "%";
        }

        @Override
        protected void exec(Decimal128 right) {
            decimal.modulo(right);
        }
    }

    private static class Decimal256Func extends ArithmeticDecimal256Function {

        public Decimal256Func(Function left, Function right, int targetType, int position) {
            super(left, right, targetType, position);
        }

        @Override
        public String getName() {
            return "%";
        }

        @Override
        protected void exec(Decimal256 right) {
            decimal.modulo(right);
        }
    }

    private static class Decimal64Func extends ArithmeticDecimal64Function {

        public Decimal64Func(Function left, Function right, int targetType, int position) {
            super(left, right, targetType, position);
        }

        @Override
        public String getName() {
            return "%";
        }

        @Override
        protected void exec(long rightValue, int rightScale) {
            decimal.modulo(rightValue, rightScale);
        }
    }
}
