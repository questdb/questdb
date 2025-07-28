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
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class DoubleArrayStdDevFunctionFactory implements FunctionFactory {
    private static final String FUNCTION_NAME = "array_stddev";

    @Override
    public String getSignature() {
        return FUNCTION_NAME + "(D[])";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new Func(args.getQuick(0));
    }

    static class Func extends DoubleFunction implements UnaryFunction, DoubleUnaryArrayAccessor {

        private final Function arrayArg;
        private int count = 0;
        private double sum = 0d;
        private double sumOfSquares = 0d;

        Func(Function arrayArg) {
            this.arrayArg = arrayArg;
        }

        @Override
        public void applyToElement(ArrayView view, int index) {
            double v = view.getDouble(index);
            if (Numbers.isFinite(v)) {
                sum += v;
                sumOfSquares += v * v;
                count++;
            }
        }

        @Override
        public void applyToEntireVanillaArray(ArrayView view) {
            FlatArrayView flatView = view.flatView();
            int offset = view.getFlatViewOffset();
            int length = view.getFlatViewLength();
            for (int i = offset, n = offset + length; i < n; i++) {
                double v = flatView.getDoubleAtAbsIndex(i);
                if (Numbers.isFinite(v)) {
                    sum += v;
                    sumOfSquares += v * v;
                    count++;
                }
            }
        }

        @Override
        public void applyToNullArray() {
            sum = Double.NaN;
        }

        @Override
        public Function getArg() {
            return arrayArg;
        }

        @Override
        public double getDouble(Record rec) {
            ArrayView arr = arrayArg.getArray(rec);
            if (arr.isNull()) {
                return Double.NaN;
            }
            sum = 0d;
            sumOfSquares = 0d;
            count = 0;

            calculate(arr);

            if (count == 0) {
                return Double.NaN;
            }
            if (count == 1) {
                return 0d; // Standard deviation of a single element is 0
            }

            // Calculate sample standard deviation: sqrt((sum(x^2) - sum(x)^2/n) / (n - 1))
            double mean = sum / count;
            double variance = (sumOfSquares - sum * mean) / (count - 1);

            // Handle potential floating point precision issues
            if (variance < 0d) {
                variance = 0d;
            }
            return Math.sqrt(variance);
        }

        @Override
        public String getName() {
            return FUNCTION_NAME;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }
}
