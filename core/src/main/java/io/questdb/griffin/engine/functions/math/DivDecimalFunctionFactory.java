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
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

import java.math.RoundingMode;

public class DivDecimalFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "/(ΞΞ)";
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
        final int precision = Decimals.MAX_PRECISION;
        final int scale = Math.min(ColumnType.getDecimalScale(leftType) + ColumnType.getDecimalScale(rightType), Decimals.MAX_SCALE);
        return new Func(left, right, ColumnType.getDecimalType(precision, scale), position);
    }

    private static class Func extends ArithmeticDecimal256Function {

        public Func(Function left, Function right, int targetType, int position) {
            super(left, right, targetType, position);
        }

        @Override
        public String getName() {
            return "/";
        }

        @Override
        protected void exec(Decimal256 right) {
            decimal.divide(right, scale, RoundingMode.HALF_EVEN);
        }
    }
}
