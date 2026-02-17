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

public class DoubleArrayAvgFunctionFactory implements FunctionFactory {
    private static final String FUNCTION_NAME = "array_avg";

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

    static class Func extends DoubleFunction implements UnaryFunction {
        private final Function arrayArg;
        private int count;
        private double sum;

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
            boolean vanilla = arr.isVanilla();
            if (arr.isNull()) {
                return Double.NaN;
            } else if (vanilla) {
                return arr.flatView().avgDouble(arr.getFlatViewOffset(), arr.getFlatViewLength());
            }
            count = 0;
            sum = 0d;
            calculateRecursive(arr, 0, 0);
            return sum / count;
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
            final int dimLen = view.getDimLen(dim);
            final int stride = view.getStride(dim);
            final boolean atDeepestDim = dim == view.getDimCount() - 1;
            if (atDeepestDim) {
                for (int i = 0; i < dimLen; i++) {
                    double v = view.getDouble(flatIndex);
                    if (Numbers.isFinite(v)) {
                        sum += v;
                        count++;
                    }
                    flatIndex += stride;
                }
            } else {
                for (int i = 0; i < dimLen; i++) {
                    calculateRecursive(view, dim + 1, flatIndex);
                    flatIndex += stride;
                }
            }
        }
    }
}
