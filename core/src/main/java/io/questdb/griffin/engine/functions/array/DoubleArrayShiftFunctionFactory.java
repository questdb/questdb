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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.WeakDimsArrayFunction;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class DoubleArrayShiftFunctionFactory implements FunctionFactory {
    private static final String FUNCTION_NAME = "shift";

    @Override
    public String getSignature() {
        return FUNCTION_NAME + "(D[]ID)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new Func(configuration, args.getQuick(0), args.getQuick(1), args.getQuick(2), position);
    }

    static void applyRecursive(ArrayView view, int dim, int flatIndex, int offset, MemoryA memory, double defaultValue) {
        final int count = view.getDimLen(dim);
        final int stride = view.getStride(dim);
        final boolean atDeepestDim = dim == view.getDimCount() - 1;
        if (atDeepestDim) {
            if (offset > 0) {
                for (int i = 0; i < count; i++) {
                    if (i >= offset) {
                        memory.putDouble(view.getDouble(flatIndex));
                        flatIndex += stride;
                    } else {
                        memory.putDouble(defaultValue);
                    }
                }
            } else {
                int absOffset = offset + count;
                flatIndex -= offset * stride;
                for (int i = 0; i < count; i++) {
                    if (i < absOffset) {
                        memory.putDouble(view.getDouble(flatIndex));
                        flatIndex += stride;
                    } else {
                        memory.putDouble(defaultValue);
                    }
                }
            }
        } else {
            for (int i = 0; i < count; i++) {
                applyRecursive(view, dim + 1, flatIndex, offset, memory, defaultValue);
                flatIndex += stride;
            }
        }
    }

    static void applyToEntireVanillaArray(ArrayView view, MemoryA memory, int offset, double defaultValue) {
        int lastDim = view.getDimLen(view.getDimCount() - 1);
        if (offset > 0) {
            for (int i = 0, n = view.getFlatViewLength(); i < n; i++) {
                if (i % lastDim >= offset) {
                    memory.putDouble(view.getDouble(i - offset));
                } else {
                    memory.putDouble(defaultValue);
                }
            }
        } else {
            int absOffset = offset + lastDim;
            for (int i = 0, n = view.getFlatViewLength(); i < n; i++) {
                if (i % lastDim < absOffset) {
                    memory.putDouble(view.getDouble(i - offset));
                } else {
                    memory.putDouble(defaultValue);
                }
            }
        }
    }

    private static class Func extends WeakDimsArrayFunction implements TernaryFunction {
        private final DirectArray array;
        private final Function arrayArg;
        private final Function defaultValueFunction;
        private final Function shiftFunction;

        public Func(CairoConfiguration configuration, Function arrayArg, Function shiftFunction, Function defaultValueFunction, int position) {
            this.arrayArg = arrayArg;
            this.shiftFunction = shiftFunction;
            this.defaultValueFunction = defaultValueFunction;
            this.type = arrayArg.getType();
            this.array = new DirectArray(configuration);
            this.position = position;
        }

        @Override
        public void close() {
            TernaryFunction.super.close();
            Misc.free(array);
        }

        @Override
        public ArrayView getArray(Record rec) {
            ArrayView arr = arrayArg.getArray(rec);
            if (arr.isNull()) {
                array.ofNull();
                return array;
            }

            int offset = shiftFunction.getInt(rec);
            if (offset == 0) {
                return arr;
            }
            MemoryA memory = array.copyShapeAndStartMemoryA(arr);
            double defaultValue = defaultValueFunction.getDouble(rec);
            if (arr.isVanilla()) {
                applyToEntireVanillaArray(arr, memory, offset, defaultValue);
            } else {
                applyRecursive(arr, 0, 0, offset, memory, defaultValue);
            }
            return array;
        }

        @Override
        public Function getCenter() {
            return shiftFunction;
        }

        @Override
        public Function getLeft() {
            return arrayArg;
        }

        @Override
        public String getName() {
            return FUNCTION_NAME;
        }

        @Override
        public Function getRight() {
            return defaultValueFunction;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            TernaryFunction.super.init(symbolTableSource, executionContext);
            this.type = arrayArg.getType();
            validateAssignedType();
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }
}
