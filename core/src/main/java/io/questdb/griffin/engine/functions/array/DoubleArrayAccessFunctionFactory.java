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
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class DoubleArrayAccessFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "[](D[]IV)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> inputArgs,
            @Transient IntList inputArgPositions,
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
            validateArgs(inputArgs, inputArgPositions);
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
                argPositions = new IntList(inputArgPositions);
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
                }
            }
            if (resultNDims == 0) {
                boolean indexArgsAreConstant = true;
                for (int n = args.size(), i = 0; i < n; i++) {
                    if (!args.getQuick(i).isConstant()) {
                        indexArgsAreConstant = false;
                        break;
                    }
                }
                if (indexArgsAreConstant) {
                    IntList indexArgs = new IntList(args.size());
                    for (int n = args.size(), i = 0; i < n; i++) {
                        indexArgs.add(args.getQuick(i).getInt(null));
                    }
                    Misc.freeObjList(args);
                    return new AccessDoubleArrayConstantIndexFunction(arrayArg, indexArgs, argPositions);
                }
                return new AccessDoubleArrayFunction(arrayArg, args, argPositions);
            }
            return new SliceDoubleArrayFunction(arrayArg, resultNDims, args, argPositions);
        } catch (Throwable e) {
            Misc.free(arrayArg);
            Misc.freeObjList(inputArgs);
            throw e;
        }
    }

    private static int flatIndexDelta(ArrayView array, int dim, int indexAtDim, int argPos) {
        int strideAtDim = array.getStride(dim);
        int dimLen = array.getDimLen(dim);
        if (indexAtDim < 0) {
            throw CairoException.nonCritical()
                    .position(argPos)
                    .put("array index must be positive [dim=").put(dim + 1)
                    .put(", index=").put(indexAtDim + 1)
                    .put(", dimLen=").put(dimLen)
                    .put(']');
        }
        if (indexAtDim >= dimLen) {
            return Numbers.INT_NULL;
        }
        return strideAtDim * indexAtDim;
    }

    private static boolean isIndexArg(int argType) {
        return argType == ColumnType.INT || argType == ColumnType.SHORT || argType == ColumnType.BYTE;
    }

    private static void validateArgs(ObjList<Function> args, IntList argPositions) throws SqlException {
        for (int n = args.size(), i = 0; i < n; i++) {
            Function arg = args.getQuick(i);
            int argType = arg.getType();
            if (!isIndexArg(argType) && !ColumnType.isInterval(argType)) {
                throw SqlException.position(argPositions.get(i))
                        .put("invalid type for array access [type=").put(argType).put(']');
            }
            if (!arg.isConstant()) {
                continue;
            }
            if (ColumnType.isInterval(argType)) {
                Interval range = arg.getInterval(null);
                long lo = range.getLo();
                long hi = range.getHi();
                if (lo < 1) {
                    throw SqlException.position(argPositions.getQuick(i))
                            .put("array slice bounds must be positive [dim=").put(i + 1)
                            .put(", lowerBound=").put(lo)
                            .put(']');
                } else if (hi < 1 && hi != Numbers.LONG_NULL) {
                    throw SqlException.position(argPositions.getQuick(i))
                            .put("array slice bounds must be positive [dim=").put(i + 1)
                            .put(", upperBound=").put(hi)
                            .put(']');
                }
            } else {
                int index = arg.getInt(null);
                if (index < 1) {
                    throw SqlException.position(argPositions.getQuick(i))
                            .put("array index must be positive [dim=").put(i + 1)
                            .put(", index=").put(index)
                            .put(']');
                }
            }
        }
    }

    private static class AccessDoubleArrayConstantIndexFunction extends DoubleFunction implements UnaryFunction {
        private final Function arrayArg;
        private final IntList indexArgPositions;
        private final IntList indexArgs;

        private AccessDoubleArrayConstantIndexFunction(Function arrayArg, IntList indexArgs, IntList indexArgPositions) {
            this.arrayArg = arrayArg;
            this.indexArgs = indexArgs;
            this.indexArgPositions = indexArgPositions;
        }

        @Override
        public Function getArg() {
            return arrayArg;
        }

        @Override
        public double getDouble(Record rec) {
            ArrayView array = arrayArg.getArray(rec);
            if (array.isNull()) {
                return Double.NaN;
            }
            int flatIndex = 0;
            for (int n = indexArgs.size(), dim = 0; dim < n; dim++) {
                // Decrement the index in the argument because Postgres uses 1-based array indexing
                int indexAtDim = indexArgs.get(dim) - 1;
                int indexDelta = flatIndexDelta(array, dim, indexAtDim, indexArgPositions.get(dim));
                if (indexDelta == Numbers.INT_NULL) {
                    return Double.NaN;
                }
                flatIndex += indexDelta;
            }
            return array.getDouble(flatIndex);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arrayArg).val('[');
            String comma = "";
            for (int n = indexArgs.size(), i = 0; i < n; i++) {
                sink.val(comma).val(indexArgs.getQuick(i));
                comma = ",";
            }
            sink.val(']');
        }
    }

    private static class AccessDoubleArrayFunction extends DoubleFunction implements MultiArgFunction {
        private final ObjList<Function> allArgs; // holds arrayArg + indexArgs
        private final Function arrayArg;
        private final IntList indexArgPositions;
        private final ObjList<Function> indexArgs;

        private AccessDoubleArrayFunction(Function arrayArg, ObjList<Function> indexArgs, IntList indexArgPositions) {
            this.arrayArg = arrayArg;
            this.indexArgs = indexArgs;
            this.indexArgPositions = indexArgPositions;
            this.allArgs = new ObjList<>(indexArgs.size() + 1);
            allArgs.add(arrayArg);
            allArgs.addAll(indexArgs);
        }

        @Override
        public ObjList<Function> getArgs() {
            return allArgs;
        }

        @Override
        public double getDouble(Record rec) {
            ArrayView array = arrayArg.getArray(rec);
            if (array.isNull()) {
                return Double.NaN;
            }
            int flatIndex = 0;
            for (int n = indexArgs.size(), dim = 0; dim < n; dim++) {
                // Decrement the index in the argument because Postgres uses 1-based array indexing
                int indexAtDim = indexArgs.getQuick(dim).getInt(rec) - 1;
                int indexDelta = flatIndexDelta(array, dim, indexAtDim, indexArgPositions.get(dim));
                if (indexDelta == Numbers.INT_NULL) {
                    return Double.NaN;
                }
                flatIndex += indexDelta;
            }
            return array.getDouble(flatIndex);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arrayArg).val('[');
            String comma = "";
            for (int n = indexArgs.size(), i = 0; i < n; i++) {
                sink.val(comma).val(indexArgs.getQuick(i));
                comma = ",";
            }
            sink.val(']');
        }
    }

    private static class SliceDoubleArrayFunction extends ArrayFunction implements MultiArgFunction {
        private final ObjList<Function> allArgs; // holds arrayArg + rangeArgs
        private final IntList argPositions;
        private final Function arrayArg;
        private final DerivedArrayView derivedArray = new DerivedArrayView();
        private final ObjList<Function> rangeArgs;

        public SliceDoubleArrayFunction(Function arrayArg, int resultNDims, ObjList<Function> rangeArgs, IntList argPositions) {
            this.arrayArg = arrayArg;
            this.rangeArgs = rangeArgs;
            this.argPositions = argPositions;
            this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, resultNDims);
            this.allArgs = new ObjList<>(rangeArgs.size() + 1);
            allArgs.add(arrayArg);
            allArgs.addAll(rangeArgs);
        }

        @Override
        public ObjList<Function> getArgs() {
            return allArgs;
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
                if (ColumnType.isInterval(rangeFn.getType())) {
                    Interval range = rangeFn.getInterval(rec);
                    long loLong = range.getLo();
                    long hiLong = range.getHi();
                    int lo = (int) loLong;
                    int hi = (int) hiLong;
                    if (hiLong == Numbers.LONG_NULL) {
                        hi = Integer.MAX_VALUE;
                    } else {
                        assert hi == hiLong : "int overflow on interval upper bound: " + hiLong;
                        // Decrement the index in the argument because Postgres uses 1-based array indexing
                        hi--;
                    }
                    assert lo == loLong : "int overflow on interval lower bound: " + loLong;
                    // Decrement the index in the argument because Postgres uses 1-based array indexing
                    lo--;
                    derivedArray.slice(dim++, lo, hi, argPos);
                } else {
                    // Decrement the index in the argument because Postgres uses 1-based array indexing
                    int index = rangeFn.getInt(rec) - 1;
                    int dimLen = derivedArray.getDimLen(dim);
                    if (index < 0) {
                        throw CairoException.nonCritical()
                                .position(argPos)
                                .put("array index must be positive [dim=").put(i + 1)
                                .put(", index=").put(index + 1)
                                .put(", dimLen=").put(dimLen)
                                .put(']');
                    }
                    derivedArray.subArray(dim, index, argPos);
                }
            }
            return derivedArray;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arrayArg).val('[');
            String comma = "";
            for (int n = rangeArgs.size(), i = 0; i < n; i++) {
                sink.val(comma).val(rangeArgs.getQuick(i));
                comma = ",";
            }
            sink.val(']');
        }
    }
}
