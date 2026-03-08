/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

/**
 * Array std dev implementation follows the previous work in
 * {@linkplain io.questdb.griffin.engine.functions.groupby.AbstractStdDevGroupByFunction GROUP BY aggregation}.
 */
public class DoubleArrayStdDevSampFunctionFactory implements FunctionFactory {
    private static final String FUNCTION_NAME = "array_stddev_samp";

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

    protected static class Func extends DoubleFunction implements UnaryFunction {

        private final Function arrayArg;
        protected int count;
        private double deltaSquaredSum;
        private double mean;

        Func(Function arrayArg) {
            this.arrayArg = arrayArg;
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
            deltaSquaredSum = 0.0;
            mean = 0.0;
            count = 0;

            if (arr.isVanilla()) {
                FlatArrayView flatView = arr.flatView();
                int offset = arr.getFlatViewOffset();
                int length = arr.getFlatViewLength();
                for (int i = offset, n = offset + length; i < n; i++) {
                    double value = flatView.getDoubleAtAbsIndex(i);
                    if (Numbers.isFinite(value)) {
                        count++;
                        double oldMean = mean;
                        mean += (value - mean) / count;
                        deltaSquaredSum += (value - mean) * (value - oldMean);
                    }
                }
            } else {
                calculateRecursive(arr, 0, 0);
            }

            long countForVariance = getCountForVariance();
            if (countForVariance <= 0) {
                return Double.NaN;
            }

            // Calculate sample standard deviation: sqrt((sum(x^2) - sum(x)^2/n) / (n - 1))
            double variance = deltaSquaredSum / countForVariance;

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

        private void calculateRecursive(ArrayView view, int dim, int flatIndex) {
            final int count = view.getDimLen(dim);
            final int stride = view.getStride(dim);
            final boolean atDeepestDim = dim == view.getDimCount() - 1;
            if (atDeepestDim) {
                for (int i = 0; i < count; i++) {
                    double value = view.getDouble(flatIndex);
                    if (Numbers.isFinite(value)) {
                        this.count++;
                        double oldMean = mean;
                        mean += (value - mean) / this.count;
                        deltaSquaredSum += (value - mean) * (value - oldMean);
                    }
                    flatIndex += stride;
                }
            } else {
                for (int i = 0; i < count; i++) {
                    calculateRecursive(view, dim + 1, flatIndex);
                    flatIndex += stride;
                }
            }
        }

        protected int getCountForVariance() {
            return count - 1;
        }
    }
}
