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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class DoubleArrayReverseFunctionFactory implements FunctionFactory {
    private static final String FUNCTION_NAME = "array_reverse";

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
        return new Func(configuration, args.getQuick(0), position);
    }

    private static void reverseRecursive(ArrayView view, int dim, int flatIndex, MemoryA memory) {
        final int count = view.getDimLen(dim);
        final int stride = view.getStride(dim);
        final boolean atDeepestDim = dim == view.getDimCount() - 1;
        if (atDeepestDim) {
            // write elements in reverse order
            int lastIndex = flatIndex + (count - 1) * stride;
            for (int i = 0; i < count; i++) {
                memory.putDouble(view.getDouble(lastIndex));
                lastIndex -= stride;
            }
        } else {
            for (int i = 0; i < count; i++) {
                reverseRecursive(view, dim + 1, flatIndex, memory);
                flatIndex += stride;
            }
        }
    }

    private static void reverseVanilla(ArrayView view, MemoryA memory) {
        FlatArrayView flatView = view.flatView();
        int lastDim = view.getDimLen(view.getDimCount() - 1);
        for (int sliceStart = view.getLo(), total = view.getHi(); sliceStart < total; sliceStart += lastDim) {
            for (int i = lastDim - 1; i >= 0; i--) {
                memory.putDouble(flatView.getDoubleAtAbsIndex(sliceStart + i));
            }
        }
    }

    private static class Func extends WeakDimsArrayFunction implements UnaryFunction {
        private final DirectArray array;
        private final Function arrayArg;

        public Func(CairoConfiguration configuration, Function arrayArg, int position) {
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

            MemoryA memory = array.copyShapeAndStartMemoryA(arr);
            int lastDim = arr.getDimLen(arr.getDimCount() - 1);
            if (lastDim == 0) {
                return array;
            }
            if (arr.isVanilla()) {
                reverseVanilla(arr, memory);
            } else {
                reverseRecursive(arr, 0, 0, memory);
            }
            return array;
        }

        @Override
        public String getName() {
            return FUNCTION_NAME;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            this.type = arrayArg.getType();
            validateAssignedType();
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("array_reverse(").val(arrayArg).val(')');
        }
    }
}
