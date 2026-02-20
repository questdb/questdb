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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class BuildArrayFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "array_build(lV)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        int argCount = args == null ? 0 : args.size();
        if (argCount < 3) {
            throw SqlException.$(position, "array_build requires at least 3 arguments: nArrays, size, filler(s)");
        }

        // arg 0: nArrays (constant long)
        Function nArraysFunc = args.getQuick(0);
        int nArraysPos = argPositions.getQuick(0);
        if (!ColumnType.isSameOrBuiltInWideningCast(nArraysFunc.getType(), ColumnType.LONG)) {
            throw SqlException.$(nArraysPos, "nArrays must be an integer");
        }
        if (!nArraysFunc.isConstant()) {
            throw SqlException.$(nArraysPos, "nArrays must be a constant");
        }
        long nArraysLong = nArraysFunc.getLong(null);
        if (nArraysLong < 1 || nArraysLong > ArrayView.DIM_MAX_LEN) {
            throw SqlException.$(nArraysPos, "nArrays out of range [nArrays=").put(nArraysLong)
                    .put(", max=").put(ArrayView.DIM_MAX_LEN).put(']');
        }
        int nArrays = (int) nArraysLong;

        // validate arg count: 1 (nArrays) + 1 (size) + nArrays (fillers) = 2 + nArrays
        int expectedArgCount = 2 + nArrays;
        if (argCount != expectedArgCount) {
            throw SqlException.$(position, "array_build with nArrays=").put(nArrays)
                    .put(" requires ").put(expectedArgCount)
                    .put(" arguments, got ").put(argCount);
        }

        // arg 1: size (int/long or DOUBLE[])
        Function sizeFunc = args.getQuick(1);
        int sizePos = argPositions.getQuick(1);
        int sizeType = sizeFunc.getType();
        boolean sizeIsArray = ColumnType.isArray(sizeType);
        if (sizeIsArray) {
            int sizeDims = ColumnType.decodeWeakArrayDimensionality(sizeType);
            if (sizeDims != 1 || ColumnType.decodeArrayElementType(sizeType) != ColumnType.DOUBLE) {
                throw SqlException.$(sizePos, "size array argument must be DOUBLE[]");
            }
        } else if (!ColumnType.isSameOrBuiltInWideningCast(sizeType, ColumnType.LONG)) {
            throw SqlException.$(sizePos, "size must be an integer or a DOUBLE[] array");
        }

        // args 2..2+nArrays-1: fillers
        ObjList<Function> allArgs = new ObjList<>(args);
        IntList allArgPositions = new IntList(argPositions);

        boolean[] fillerIsArray = new boolean[nArrays];
        for (int i = 0; i < nArrays; i++) {
            int argIndex = 2 + i;
            Function fillerFunc = args.getQuick(argIndex);
            int fillerPos = argPositions.getQuick(argIndex);
            int fillerType = fillerFunc.getType();
            if (ColumnType.isArray(fillerType)) {
                int fillerDims = ColumnType.decodeWeakArrayDimensionality(fillerType);
                if (fillerDims != 1 || ColumnType.decodeArrayElementType(fillerType) != ColumnType.DOUBLE) {
                    throw SqlException.$(fillerPos, "filler array argument must be DOUBLE[]");
                }
                fillerIsArray[i] = true;
            } else if (!ColumnType.isSameOrBuiltInWideningCast(fillerType, ColumnType.DOUBLE)) {
                throw SqlException.$(fillerPos, "filler must be a numeric value or a DOUBLE[] array");
            }
        }

        int returnType = ColumnType.encodeArrayType(ColumnType.DOUBLE, nArrays == 1 ? 1 : 2);
        return new BuildArrayFunc(
                configuration,
                allArgs,
                allArgPositions,
                nArrays,
                sizeIsArray,
                fillerIsArray,
                returnType,
                position
        );
    }

    private static class BuildArrayFunc extends ArrayFunction implements MultiArgFunction {
        private final @NotNull IntList argPositions;
        private final @NotNull ObjList<Function> args;
        private DirectArray array;
        private final boolean[] fillerIsArray;
        private final int nArrays;
        private final int position;
        private final boolean sizeIsArray;

        BuildArrayFunc(
                CairoConfiguration configuration,
                @NotNull ObjList<Function> args,
                @NotNull IntList argPositions,
                int nArrays,
                boolean sizeIsArray,
                boolean[] fillerIsArray,
                int returnType,
                int position
        ) {
            try {
                this.args = args;
                this.argPositions = argPositions;
                this.nArrays = nArrays;
                this.sizeIsArray = sizeIsArray;
                this.fillerIsArray = fillerIsArray;
                this.type = returnType;
                this.position = position;
                this.array = new DirectArray(configuration);
                this.array.setType(returnType);
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public @NotNull ObjList<Function> args() {
            return args;
        }

        @Override
        public void close() {
            MultiArgFunction.super.close();
            array = Misc.free(array);
        }

        @Override
        public ArrayView getArray(Record rec) {
            Function sizeFunc = args.getQuick(1);
            int sizePos = argPositions.getQuick(1);

            // evaluate size
            int size;
            if (sizeIsArray) {
                ArrayView sizeArr = sizeFunc.getArray(rec);
                if (sizeArr.isNull()) {
                    array.ofNull();
                    return array;
                }
                size = sizeArr.getCardinality();
                if (size > ArrayView.DIM_MAX_LEN) {
                    throw CairoException.nonCritical().position(sizePos)
                            .put("size exceeds maximum [size=").put(size)
                            .put(", max=").put(ArrayView.DIM_MAX_LEN).put(']');
                }
            } else {
                long sizeLong = sizeFunc.getLong(rec);
                if (sizeLong == Numbers.LONG_NULL) {
                    array.ofNull();
                    return array;
                }
                if (sizeLong < 0) {
                    throw CairoException.nonCritical().position(sizePos)
                            .put("size must not be negative [size=").put(sizeLong).put(']');
                }
                if (sizeLong > ArrayView.DIM_MAX_LEN) {
                    throw CairoException.nonCritical().position(sizePos)
                            .put("size exceeds maximum [size=").put(sizeLong)
                            .put(", max=").put(ArrayView.DIM_MAX_LEN).put(']');
                }
                size = (int) sizeLong;
            }

            // set up shape (setType reinitializes the shape list after a prior ofNull)
            array.setType(type);
            if (nArrays == 1) {
                array.setDimLen(0, size);
            } else {
                array.setDimLen(0, nArrays);
                array.setDimLen(1, size);
            }
            array.applyShape(position);

            // populate data
            MemoryA mem = array.startMemoryA();
            for (int i = 0; i < nArrays; i++) {
                Function fillerFunc = args.getQuick(2 + i);
                if (fillerIsArray[i]) {
                    ArrayView src = fillerFunc.getArray(rec);
                    if (src.isNull()) {
                        for (int j = 0; j < size; j++) {
                            mem.putDouble(Double.NaN);
                        }
                    } else {
                        int srcLen = src.getCardinality();
                        if (srcLen == size) {
                            // bulk copy when sizes match
                            src.appendDataToMem(mem);
                        } else {
                            int copyLen = Math.min(srcLen, size);
                            for (int j = 0; j < copyLen; j++) {
                                mem.putDouble(src.getDouble(j));
                            }
                            for (int j = copyLen; j < size; j++) {
                                mem.putDouble(Double.NaN);
                            }
                        }
                    }
                } else {
                    double val = fillerFunc.getDouble(rec);
                    for (int j = 0; j < size; j++) {
                        mem.putDouble(val);
                    }
                }
            }
            return array;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("array_build").val('(');
            for (int i = 0, n = args.size(); i < n; i++) {
                if (i > 0) {
                    sink.val(',');
                }
                sink.val(args.getQuick(i));
            }
            sink.val(')');
        }
    }
}
