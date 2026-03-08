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
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class DoubleArrayMultiplyScalarFunctionFactory implements FunctionFactory {
    private static final String OPERATOR_NAME = "*";

    @Override
    public String getSignature() {
        return OPERATOR_NAME + "(D[]D)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new Func(configuration, args.getQuick(0), args.getQuick(1), position);
    }

    @Override
    public boolean shouldSwapArgs() {
        return true;
    }

    private static class Func extends WeakDimsArrayFunction implements BinaryFunction {
        private final DirectArray array;
        private final Function arrayArg;
        private final Function scalarArg;

        public Func(CairoConfiguration configuration, Function arrayArg, Function scalarArg, int position) {
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

            final double scalarValue = scalarArg.getDouble(rec);
            final MemoryA memory = array.copyShapeAndStartMemoryA(arr);
            if (arr.isVanilla()) {
                FlatArrayView flatView = arr.flatView();
                for (int i = arr.getLo(), n = arr.getHi(); i < n; i++) {
                    memory.putDouble(flatView.getDoubleAtAbsIndex(i) * scalarValue);
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
            return OPERATOR_NAME;
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

        private static void calculateRecursive(ArrayView view, int dim, int flatIndex, double scalarValue, MemoryA memOut) {
            final int count = view.getDimLen(dim);
            final int stride = view.getStride(dim);
            final boolean atDeepestDim = dim == view.getDimCount() - 1;
            if (atDeepestDim) {
                for (int i = 0; i < count; i++) {
                    memOut.putDouble(view.getDouble(flatIndex) * scalarValue);
                    flatIndex += stride;
                }
            } else {
                for (int i = 0; i < count; i++) {
                    calculateRecursive(view, dim + 1, flatIndex, scalarValue, memOut);
                    flatIndex += stride;
                }
            }
        }
    }
}
