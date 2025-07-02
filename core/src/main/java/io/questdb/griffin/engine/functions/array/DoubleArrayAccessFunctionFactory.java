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
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
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
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        validateIndexArgs(args, argPositions);

        Function arrayArg = args.getQuick(0);
        ObjList<Function> argsCopy = null;
        IntList argPositionsCopy = null;
        // If the array argument is another array slicing function, and if all its arguments are indexes
        // (not ranges for slicing), we can inline it into this function by prepending all its args to
        // our args, and by using its array argument as our array argument.
        if (arrayArg instanceof SliceDoubleArrayFunction) {
            boolean canInline = true;
            final SliceDoubleArrayFunction sliceFn = (SliceDoubleArrayFunction) arrayArg;
            final ObjList<Function> rangeArgs = sliceFn.allArgs;
            for (int n = rangeArgs.size(), i = 0; i < n; i++) {
                if (ColumnType.isInterval(rangeArgs.getQuick(i).getType())) {
                    canInline = false;
                    break;
                }
            }
            if (canInline) {
                arrayArg = sliceFn.arrayArg;
                argsCopy = new ObjList<>(rangeArgs.size() + args.size());
                argsCopy.addAll(rangeArgs); // rangeArgs includes sliceFn.arrayArg
                argPositionsCopy = new IntList();
                argPositionsCopy.addAll(sliceFn.allArgPositions); // includes sliceFn.arrayArg's position
                for (int i = 1, n = args.size(); i < n; i++) {
                    argsCopy.add(args.getQuick(i));
                    argPositionsCopy.add(argPositions.getQuick(i));
                }
            }
        }
        if (argsCopy == null) {
            argsCopy = new ObjList<>(args);
            argPositionsCopy = new IntList(argPositions);
        }

        final int nDims = ColumnType.decodeArrayDimensionality(arrayArg.getType());
        final int nArgs = argsCopy.size() - 1;
        if (nArgs > nDims) {
            throw SqlException
                    .position(argPositionsCopy.get(nDims + 1))
                    .put("too many array access arguments [nArgs=").put(nArgs)
                    .put(", nDims=").put(nDims)
                    .put(']');
        }
        int resultNDims = nDims;
        for (int i = 1, n = argsCopy.size(); i < n; i++) {
            final int argType = argsCopy.getQuick(i).getType();
            if (isIndexArg(argType)) {
                resultNDims--;
            }
        }
        if (resultNDims == 0) {
            boolean indexArgsAreConstant = true;
            for (int i = 1, n = argsCopy.size(); i < n; i++) {
                if (!argsCopy.getQuick(i).isConstant()) {
                    indexArgsAreConstant = false;
                    break;
                }
            }
            if (indexArgsAreConstant) {
                IntList indexArgs = new IntList(argsCopy.size() - 1);
                for (int i = 1, n = argsCopy.size(); i < n; i++) {
                    int index = argsCopy.getQuick(i).getInt(null);
                    if (index == Numbers.INT_NULL) {
                        return DoubleConstant.NULL;
                    }
                    indexArgs.add(index);
                    Misc.free(argsCopy.getQuick(i));
                }
                argPositionsCopy.removeIndex(0); // remove arrayArg's position
                return new AccessDoubleArrayConstantIndexFunction(arrayArg, indexArgs, argPositionsCopy);
            }
            return new AccessDoubleArrayFunction(arrayArg, argsCopy, argPositionsCopy);
        }
        return new SliceDoubleArrayFunction(arrayArg, resultNDims, argsCopy, argPositionsCopy);
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

    private static void validateIndexArgs(ObjList<Function> args, IntList argPositions) throws SqlException {
        for (int i = 1, n = args.size(); i < n; i++) {
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
                if (lo < 1 && lo != Numbers.INT_NULL) {
                    throw SqlException.position(argPositions.getQuick(i))
                            .put("array slice bounds must be positive [dim=").put(i)
                            .put(", lowerBound=").put(lo)
                            .put(']');
                } else if (hi < 1 && hi != Numbers.INT_NULL) {
                    throw SqlException.position(argPositions.getQuick(i))
                            .put("array slice bounds must be positive [dim=").put(i)
                            .put(", upperBound=").put(hi)
                            .put(']');
                }
            } else {
                int index = arg.getInt(null);
                if (index < 1 && index != Numbers.INT_NULL) {
                    throw SqlException.position(argPositions.getQuick(i))
                            .put("array index must be positive [dim=").put(i)
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
            for (int dim = 0, n = indexArgs.size(); dim < n; dim++) {
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
        private final IntList allArgPositions;
        private final ObjList<Function> allArgs; // holds [arrayArg, indexArgs...]
        private final Function arrayArg;

        private AccessDoubleArrayFunction(Function arrayArg, ObjList<Function> allArgs, IntList allArgPositions) {
            this.arrayArg = arrayArg;
            this.allArgs = allArgs;
            this.allArgPositions = allArgPositions;
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
            for (int i = 1, n = allArgs.size(); i < n; i++) {
                int pgIndexAtDim = allArgs.getQuick(i).getInt(rec);
                if (pgIndexAtDim == Numbers.INT_NULL) {
                    return Double.NaN;
                }
                // Decrement the index in the argument because Postgres uses 1-based array indexing
                int indexAtDim = pgIndexAtDim - 1;
                int indexDelta = flatIndexDelta(array, i - 1, indexAtDim, allArgPositions.get(i));
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
            for (int i = 1, n = allArgs.size(); i < n; i++) {
                sink.val(comma).val(allArgs.getQuick(i));
                comma = ",";
            }
            sink.val(']');
        }
    }

    private static class SliceDoubleArrayFunction extends ArrayFunction implements MultiArgFunction {
        private final IntList allArgPositions;
        private final ObjList<Function> allArgs; // holds [arrayArg, rangeArgs...]
        private final Function arrayArg;
        private final DerivedArrayView derivedArray = new DerivedArrayView();

        public SliceDoubleArrayFunction(Function arrayArg, int resultNDims, ObjList<Function> allArgs, IntList allArgPositions) {
            this.arrayArg = arrayArg;
            this.allArgs = allArgs;
            this.allArgPositions = allArgPositions;
            this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, resultNDims);
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
            for (int i = 1, n = allArgs.size(); i < n; i++) {
                final Function rangeFn = allArgs.getQuick(i);
                final int argPos = allArgPositions.get(i);
                if (ColumnType.isInterval(rangeFn.getType())) {
                    Interval range = rangeFn.getInterval(rec);
                    long loLong = range.getLo();
                    long hiLong = range.getHi();
                    if (loLong == Numbers.INT_NULL || hiLong == Numbers.INT_NULL) {
                        derivedArray.ofNull();
                        return derivedArray;
                    }
                    int lo = (int) loLong;
                    int hi = (int) hiLong;
                    if (hiLong == Long.MAX_VALUE) {
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
                    if (index < 0) {
                        int dimLen = derivedArray.getDimLen(dim);
                        throw CairoException.nonCritical()
                                .position(argPos)
                                .put("array index must be positive [dim=").put(dim + 1)
                                .put(", index=").put(index + 1)
                                .put(", dimLen=").put(dimLen)
                                .put(']');
                    }
                    derivedArray.subArray(dim, index, argPos);
                    if (derivedArray.isNull()) {
                        return derivedArray;
                    }
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
            for (int i = 1, n = allArgs.size(); i < n; i++) {
                sink.val(comma).val(allArgs.getQuick(i));
                comma = ",";
            }
            sink.val(']');
        }
    }
}
