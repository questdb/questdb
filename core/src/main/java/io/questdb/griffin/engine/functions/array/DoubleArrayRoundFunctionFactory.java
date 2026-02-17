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
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.WeakDimsArrayFunction;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

import static io.questdb.std.Numbers.pow10max;

public class DoubleArrayRoundFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "round(D[]I)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function arg = args.getQuick(0);
        final Function scale = args.getQuick(1);
        if (scale.isConstant()) {
            int scaleValue = scale.getInt(null);
            if (scaleValue > -1) {
                if (scaleValue + 2 < pow10max) {
                    return new PositiveScaleFunc(configuration, arg, scaleValue, position);
                }
                return new DegenerateScaleFunc(configuration, arg, position);
            }
        }
        return new VarScaleFunc(configuration, arg, scale, position);
    }

    private static class DegenerateScaleFunc extends WeakDimsArrayFunction implements UnaryFunction {
        private final DirectArray array;
        private final Function arrayArg;

        public DegenerateScaleFunc(CairoConfiguration configuration, Function arrayArg, int position) {
            this.arrayArg = arrayArg;
            this.type = arrayArg.getType();
            this.array = new DirectArray(configuration);
            this.position = position;
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

            final var memory = array.copyShapeAndStartMemoryA(arr);
            for (int i = arr.getLo(), n = arr.getHi(); i < n; i++) {
                memory.putDouble(Double.NaN);
            }
            return array;
        }

        @Override
        public String getName() {
            return "round";
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            this.type = arrayArg.getType();
            validateAssignedType();
        }

        @Override
        public boolean isOperator() {
            return true;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class PositiveScaleFunc extends WeakDimsArrayFunction implements UnaryFunction {
        private final DirectArray array;
        private final Function arrayArg;
        private final int scale;

        public PositiveScaleFunc(CairoConfiguration configuration, Function arrayArg, int scale, int position) {
            this.arrayArg = arrayArg;
            this.scale = scale;
            this.type = arrayArg.getType();
            this.array = new DirectArray(configuration);
            this.position = position;
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

            final var memory = array.copyShapeAndStartMemoryA(arr);
            if (arr.isVanilla()) {
                FlatArrayView flatView = arr.flatView();
                for (int i = arr.getLo(), n = arr.getHi(); i < n; i++) {
                    memory.putDouble(roundNc(flatView.getDoubleAtAbsIndex(i), scale));
                }
            } else {
                calculateRecursive(arr, 0, 0, scale, memory);
            }
            return array;
        }

        @Override
        public String getName() {
            return "round";
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            this.type = arrayArg.getType();
            validateAssignedType();
        }

        @Override
        public boolean isOperator() {
            return true;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        private static void calculateRecursive(ArrayView view, int dim, int flatIndex, int scale, MemoryA memOut) {
            final int count = view.getDimLen(dim);
            final int stride = view.getStride(dim);
            final boolean atDeepestDim = dim == view.getDimCount() - 1;
            if (atDeepestDim) {
                for (int i = 0; i < count; i++) {
                    memOut.putDouble(roundNc(view.getDouble(flatIndex), scale));
                    flatIndex += stride;
                }
            } else {
                for (int i = 0; i < count; i++) {
                    calculateRecursive(view, dim + 1, flatIndex, scale, memOut);
                    flatIndex += stride;
                }
            }
        }

        private static double roundNc(double d, int scale) {
            if (Numbers.isFinite(d)) {
                return Numbers.roundHalfUpPosScale(d, scale);
            }
            return Double.NaN;
        }
    }

    private static class VarScaleFunc extends WeakDimsArrayFunction implements BinaryFunction {
        private final DirectArray array;
        private final Function arrayArg;
        private final Function scalarArg;

        public VarScaleFunc(CairoConfiguration configuration, Function arrayArg, Function scalarArg, int position) {
            this.arrayArg = arrayArg;
            this.scalarArg = scalarArg;
            this.type = arrayArg.getType();
            this.array = new DirectArray(configuration);
            this.position = position;
        }

        @Override
        public void close() {
            BinaryFunction.super.close();
            Misc.free(array);
        }

        @Override
        public ArrayView getArray(Record rec) {
            ArrayView arr = arrayArg.getArray(rec);
            if (arr.isNull()) {
                array.ofNull();
                return array;
            }

            final var scalarValue = scalarArg.getInt(rec);
            final var memory = array.copyShapeAndStartMemoryA(arr);
            if (arr.isVanilla()) {
                FlatArrayView flatView = arr.flatView();
                for (int i = arr.getLo(), n = arr.getHi(); i < n; i++) {
                    memory.putDouble(roundNc(flatView.getDoubleAtAbsIndex(i), scalarValue));
                }
            } else {
                calculateRecursive(arr, 0, 0, scalarValue, memory);
            }
            return array;
        }

        @Override
        public Function getLeft() {
            return arrayArg;
        }

        @Override
        public String getName() {
            return "round";
        }

        @Override
        public Function getRight() {
            return scalarArg;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);
            this.type = arrayArg.getType();
            validateAssignedType();
        }

        @Override
        public boolean isOperator() {
            return true;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        private static void calculateRecursive(ArrayView view, int dim, int flatIndex, int scalarValue, MemoryA memOut) {
            final int count = view.getDimLen(dim);
            final int stride = view.getStride(dim);
            final boolean atDeepestDim = dim == view.getDimCount() - 1;
            if (atDeepestDim) {
                for (int i = 0; i < count; i++) {
                    memOut.putDouble(roundNc(view.getDouble(flatIndex), scalarValue));
                    flatIndex += stride;
                }
            } else {
                for (int i = 0; i < count; i++) {
                    calculateRecursive(view, dim + 1, flatIndex, scalarValue, memOut);
                    flatIndex += stride;
                }
            }
        }

        private static double roundNc(double d, int scale) {
            if (Numbers.isNull(d) || Numbers.INT_NULL == scale) {
                return Double.NaN;
            }

            try {
                return Numbers.roundHalfUp(d, scale);
            } catch (NumericException ignore) {
                return Double.NaN;
            }
        }
    }
}
