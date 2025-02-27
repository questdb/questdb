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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DerivedArrayView;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class DoubleArrayAccessFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "[](D[]IV)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> inputArgs,
            IntList inputArgPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function arrayArg = null;
        try {
            arrayArg = inputArgs.getQuick(0);
            inputArgs.remove(0);
            inputArgPositions.removeIndex(0);
            ObjList<Function> args = null;
            IntList argPositions = null;
            // If the array argument is another array slicing function, and if all its arguments are indexes
            // (not ranges for slicing), we can inline it into this function by prepending all its args to
            // our args, and by using its array argument as our array argument.
            if (arrayArg instanceof SliceDoubleArrayFunction) {
                boolean canInline = true;
                SliceDoubleArrayFunction sliceFn = (SliceDoubleArrayFunction) arrayArg;
                ObjList<Function> rangeArgs = sliceFn.rangeArgs;
                for (int n = rangeArgs.size(), i = 0; i < n; i++) {
                    if (ColumnType.isInterval(rangeArgs.getQuick(i).getType())) {
                        canInline = false;
                        break;
                    }
                }
                if (canInline) {
                    arrayArg = sliceFn.arrayArg;
                    args = new ObjList<>(rangeArgs);
                    args.addAll(inputArgs);
                    argPositions = new IntList();
                    argPositions.addAll(sliceFn.argPositions);
                    argPositions.addAll(inputArgPositions);
                }
            }
            if (args == null) {
                args = new ObjList<>(inputArgs);
                argPositions = inputArgPositions;
            }

            int nDims = ColumnType.decodeArrayDimensionality(arrayArg.getType());
            int nArgs = args.size();
            if (nArgs > nDims) {
                throw SqlException
                        .position(argPositions.get(nDims))
                        .put("too many array access arguments [nArgs=").put(nArgs)
                        .put(", nDims=").put(nDims)
                        .put(']');
            }
            int resultNDims = nDims;
            for (int n = args.size(), i = 0; i < n; i++) {
                int argType = args.getQuick(i).getType();
                if (isIndexArg(argType)) {
                    resultNDims--;
                } else if (!ColumnType.isInterval(argType)) {
                    throw SqlException.position(argPositions.get(i))
                            .put("invalid argument type [type=").put(argType).put(']');
                }
            }
            return resultNDims == 0
                    ? new AccessDoubleArrayFunction(arrayArg, args, argPositions)
                    : new SliceDoubleArrayFunction(arrayArg, resultNDims, args, argPositions);
        } catch (Throwable e) {
            Misc.free(arrayArg);
            Misc.clearObjList(inputArgs);
            throw e;
        }
    }

    private static boolean isIndexArg(int argType) {
        return argType == ColumnType.INT || argType == ColumnType.SHORT || argType == ColumnType.BYTE;
    }

    static class AccessDoubleArrayFunction extends DoubleFunction {
        final IntList indexArgPositions;
        final ObjList<Function> indexArgs;
        Function arrayArg;

        AccessDoubleArrayFunction(Function arrayArg, ObjList<Function> indexArgs, IntList indexArgPositions) {
            this.arrayArg = arrayArg;
            this.indexArgs = indexArgs;
            this.indexArgPositions = indexArgPositions;
        }

        @Override
        public void close() {
            this.arrayArg = Misc.free(arrayArg);
            for (int n = indexArgs.size(), i = 0; i < n; i++) {
                indexArgs.getQuick(i).close();
            }
            indexArgs.clear();
        }

        @Override
        public double getDouble(Record rec) {
            ArrayView array = arrayArg.getArray(rec);
            if (array.isNull()) {
                return Double.NaN;
            }
            int flatIndex = 0;
            for (int n = indexArgs.size(), dim = 0; dim < n; dim++) {
                int indexAtDim = indexArgs.getQuick(dim).getInt(rec);
                int strideAtDim = array.getStride(dim);
                int dimLen = array.getDimLen(dim);
                if (indexAtDim < 0 || indexAtDim >= dimLen) {
                    throw CairoException.nonCritical()
                            .position(indexArgPositions.get(dim))
                            .put("array index out of range [dim=").put(dim)
                            .put(", index=").put(indexAtDim)
                            .put(", dimLen=").put(dimLen).put(']');
                }
                flatIndex += strideAtDim * indexAtDim;
            }
            return array.flatView().getDouble(array.getFlatViewOffset() + flatIndex);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("[](").val(arrayArg);
            for (int n = indexArgs.size(), i = 0; i < n; i++) {
                sink.val(',').val(indexArgs.getQuick(i));
            }
            sink.val(')');
        }
    }

    static class SliceDoubleArrayFunction extends ArrayFunction {

        private final IntList argPositions;
        private final DerivedArrayView derivedArray = new DerivedArrayView();
        private final ObjList<Function> rangeArgs;
        private Function arrayArg;

        public SliceDoubleArrayFunction(Function arrayArg, int resultNDims, ObjList<Function> rangeArgs, IntList argPositions) {
            this.arrayArg = arrayArg;
            this.rangeArgs = rangeArgs;
            this.argPositions = argPositions;
            this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, resultNDims);
        }

        @Override
        public void close() {
            this.arrayArg = Misc.free(this.arrayArg);
            for (int n = rangeArgs.size(), i = 0; i < n; i++) {
                rangeArgs.getQuick(i).close();
            }
            rangeArgs.clear();
        }

        @Override
        public ArrayView getArray(Record rec) {
            ArrayView array = arrayArg.getArray(rec);
            derivedArray.of(array);
            if (derivedArray.isNull()) {
                return derivedArray;
            }
            int dim = 0;
            for (int n = rangeArgs.size(), i = 0; i < n; i++) {
                Function rangeFn = rangeArgs.getQuick(i);
                int argPos = argPositions.get(i);
                if (rangeFn.getType() == ColumnType.INTERVAL) {
                    Interval range = rangeFn.getInterval(rec);
                    long loLong = range.getLo();
                    long hiLong = range.getHi();
                    int lo = (int) loLong;
                    int hi = (int) hiLong;
                    if (hiLong == Numbers.LONG_NULL) {
                        hi = Numbers.INT_NULL;
                    } else {
                        assert hi == hiLong : "int overflow on interval upper bound: " + hiLong;
                    }
                    assert lo == loLong : "int overflow on interval lower bound: " + loLong;
                    derivedArray.slice(dim++, lo, hi, argPos);
                } else {
                    int index = rangeFn.getInt(rec);
                    int dimLen = derivedArray.getDimLen(dim);
                    if (index < 0 || index >= dimLen) {
                        throw CairoException.nonCritical()
                                .position(argPos)
                                .put("array index out of range [dim=").put(i)
                                .put(", index=").put(index)
                                .put(", dimLen=").put(dimLen)
                                .put(']');
                    }
                    derivedArray.slice(dim, index, index + 1, argPos);
                    derivedArray.removeDim(dim);
                }
            }
            return derivedArray;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("[](").val(arrayArg);
            for (int n = rangeArgs.size(), i = 0; i < n; i++) {
                sink.val(',').val(rangeArgs.getQuick(i));
            }
            sink.val(')');
        }
    }
}
