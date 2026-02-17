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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class DoubleArrayCumSumFunctionFactory implements FunctionFactory {
    private static final String FUNCTION_NAME = "array_cum_sum";

    @Override
    public String getSignature() {
        return FUNCTION_NAME + "(D[])";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new Func(configuration, args.getQuick(0));
    }

    private static class Func extends ArrayFunction implements UnaryFunction {
        private final DirectArray array;
        private final Function arrayArg;
        private double compensation = 0d;
        private double currentSum;
        private MemoryA memory;

        public Func(CairoConfiguration configuration, Function arrayArg) {
            this.arrayArg = arrayArg;
            this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            this.array = new DirectArray(configuration);
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            Misc.free(array);
        }

        @Override
        public Function getArg() {
            return arrayArg;
        }

        @Override
        public ArrayView getArray(Record rec) {
            ArrayView arr = arrayArg.getArray(rec);
            if (arr.isNull()) {
                array.ofNull();
                return array;
            }

            currentSum = Double.NaN;
            compensation = 0d;
            array.setType(type);
            array.setDimLen(0, arr.getCardinality());
            array.applyShape();
            memory = array.startMemoryA();
            if (arr.isNull()) {
                array.ofNull();
            } else if (arr.isVanilla()) {
                FlatArrayView flatView = arr.flatView();
                for (int i = arr.getLo(), n = arr.getHi(); i < n; i++) {
                    accumulate(flatView.getDoubleAtAbsIndex(i));
                }
                if (compensation == 0d && Numbers.isNull(currentSum)) {
                    // no non-null values so return null
                    array.ofNull();
                }
            } else {
                calculateRecursive(arr, 0, 0);
            }
            if (compensation == 0d && Numbers.isNull(currentSum)) {
                // no non-null values so return null
                array.ofNull();
            }
            return array;
        }

        @Override
        public String getName() {
            return FUNCTION_NAME;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        private void accumulate(double v) {
            if (Numbers.isFinite(v)) {
                if (compensation == 0d && Numbers.isNull(currentSum)) {
                    currentSum = 0d;
                }
                final double y = v - compensation;
                final double t = currentSum + y;
                compensation = t - currentSum - y;
                currentSum = t;
            }
            memory.putDouble(currentSum);
        }

        private void calculateRecursive(ArrayView view, int dim, int flatIndex) {
            final int count = view.getDimLen(dim);
            final int stride = view.getStride(dim);
            final boolean atDeepestDim = dim == view.getDimCount() - 1;
            if (atDeepestDim) {
                for (int i = 0; i < count; i++) {
                    accumulate(view.getDouble(flatIndex));
                    flatIndex += stride;
                }
            } else {
                for (int i = 0; i < count; i++) {
                    calculateRecursive(view, dim + 1, flatIndex);
                    flatIndex += stride;
                }
            }
        }
    }
}
