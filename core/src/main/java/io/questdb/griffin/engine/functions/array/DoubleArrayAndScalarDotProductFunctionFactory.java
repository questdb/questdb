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

package io.questdb.griffin.engine.functions.array;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class DoubleArrayAndScalarDotProductFunctionFactory implements FunctionFactory {
    private static final String FUNCTION_NAME = "dot_product";

    @Override
    public String getSignature() {
        return FUNCTION_NAME + "(D[]D)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new Func(args.getQuick(0), args.getQuick(1));
    }

    @Override
    public boolean shouldSwapArgs() {
        return true;
    }

    private static class Func extends DoubleFunction implements BinaryFunction, DoubleUnaryArrayAccessor {
        private final Function leftArg;
        private final Function rightArg;
        private double scalar;
        private double value;

        public Func(Function leftArg, Function rightArg) {
            this.leftArg = leftArg;
            this.rightArg = rightArg;
        }

        @Override
        public void applyToElement(ArrayView view, int index) {
            double v = view.getDouble(index);
            if (!Double.isNaN(v)) {
                value += v * scalar;
            }
        }

        @Override
        public void applyToEntireVanillaArray(ArrayView view) {
            FlatArrayView flatView = view.flatView();
            for (int i = view.getFlatViewOffset(), n = view.getFlatViewOffset() + view.getFlatViewLength(); i < n; i++) {
                double v = flatView.getDoubleAtAbsIndex(i);
                if (!Double.isNaN(v)) {
                    value += v * scalar;
                }
            }
        }

        @Override
        public void applyToNullArray() {
        }

        @Override
        public double getDouble(Record rec) {
            ArrayView arr = leftArg.getArray(rec);
            if (arr.isNull()) {
                return Double.NaN;
            }
            scalar = rightArg.getDouble(rec);
            if (Double.isNaN(scalar)) {
                return Double.NaN;
            }
            value = 0d;
            calculate(arr);
            return value;
        }

        @Override
        public Function getLeft() {
            return leftArg;
        }

        @Override
        public String getName() {
            return FUNCTION_NAME;
        }

        @Override
        public Function getRight() {
            return rightArg;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }
}
