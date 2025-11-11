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
        return "[](D[]LV)";
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
        if (arrayArg instanceof SliceDoubleArrayFunction sliceFn) {
            boolean canInline = true;
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

        final int nDims = ColumnType.decodeWeakArrayDimensionality(arrayArg.getType());
        if (nDims == -1) {
            throw SqlException.position(argPositions.get(0)).put("array bind variable access is not supported");
        }
        final int nArgs = argsCopy.size() - 1;
        if (nArgs > nDims) {
            throw SqlException
                    .position(argPositions.get(nDims))
                    .put("too many array access arguments [nDims=").put(nDims)
                    .put(", nArgs=").put(nArgs)
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
                    long index = argsCopy.getQuick(i).getLong(null);
                    if (index == Numbers.LONG_NULL) {
                        return DoubleConstant.NULL;
                    }
                    indexArgs.add((int) index);
                    Misc.free(argsCopy.getQuick(i));
                }
                argPositionsCopy.removeIndex(0); // remove arrayArg's position
                return new AccessDoubleArrayConstantIndexFunction(arrayArg, indexArgs, argPositionsCopy);
            }
            return new AccessDoubleArrayFunction(arrayArg, argsCopy, argPositionsCopy);
        }
        return new SliceDoubleArrayFunction(arrayArg, resultNDims, argsCopy, argPositionsCopy);
    }

    private static int flatIndexDelta(ArrayView array, int dim, int pgIndexAtDim) {
        int strideAtDim = array.getStride(dim);
        int dimLen = array.getDimLen(dim);
        int indexAtDim;
        if (pgIndexAtDim < 0) {
            indexAtDim = pgIndexAtDim + dimLen; // This automatically accounts for 1-based indexing
        } else {
            // Decrement the index in the argument because Postgres uses 1-based array indexing
            indexAtDim = pgIndexAtDim - 1;
        }
        if (indexAtDim < 0 || indexAtDim >= dimLen) {
            return Numbers.INT_NULL;
        }
        return strideAtDim * indexAtDim;
    }

    private static boolean isIndexArg(int argType) {
        return ColumnType.isAssignableFrom(argType, ColumnType.LONG);
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
                if (lo == 0 || hi == 0) {
                    throw SqlException.position(argPositions.getQuick(i))
                            .put("array slice bounds must be non-zero [dim=").put(i)
                            .put(", lowerBound=").put(lo)
                            .put(", upperBound=").put(hi)
                            .put(']');
                }
            } else {
                long indexLong = arg.getLong(null);
                if (indexLong == Numbers.LONG_NULL) {
                    continue;
                }
                int index = (int) indexLong;
                if (index != indexLong) {
                    throw SqlException.position(argPositions.getQuick(i))
                            .put("int overflow on array index [dim=").put(i)
                            .put(", index=").put(indexLong)
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
                int pgIndexAtDim = indexArgs.get(dim);
                if (pgIndexAtDim == 0) {
                    throw CairoException.nonCritical()
                            .position(indexArgPositions.get(dim))
                            .put("array index must be non-zero [dim=").put(dim + 1)
                            .put(", index=").put(pgIndexAtDim)
                            .put(']');
                }
                int indexDelta = flatIndexDelta(array, dim, pgIndexAtDim);
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
        public ObjList<Function> args() {
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
                long pgIndexAtDimLong = allArgs.getQuick(i).getLong(rec);
                if (pgIndexAtDimLong == Numbers.LONG_NULL) {
                    return Double.NaN;
                }
                if (pgIndexAtDimLong == 0) {
                    throw CairoException.nonCritical()
                            .position(allArgPositions.getQuick(i))
                            .put("array index must be non-zero [dim=").put(i)
                            .put(", index=").put(pgIndexAtDimLong)
                            .put(']');
                }
                int pgIndexAtDim = (int) pgIndexAtDimLong;
                if (pgIndexAtDim != pgIndexAtDimLong) {
                    throw CairoException.nonCritical().position(allArgPositions.getQuick(i))
                            .put("int overflow on array index [dim=").put(i)
                            .put(", index=").put(pgIndexAtDimLong)
                            .put(']');
                }
                int indexDelta = flatIndexDelta(array, i - 1, pgIndexAtDim);
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
        public ObjList<Function> args() {
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
                final int argPos = allArgPositions.getQuick(i);
                final int dimLen = derivedArray.getDimLen(dim);
                if (ColumnType.isInterval(rangeFn.getType())) {
                    Interval range = rangeFn.getInterval(rec);
                    long loLong = range.getLo();
                    long hiLong = range.getHi();
                    if (loLong == Numbers.INT_NULL || hiLong == Numbers.INT_NULL) {
                        derivedArray.ofNull();
                        return derivedArray;
                    }
                    int lo = toIndex(loLong, dim, dimLen, argPos, "lower");
                    int hi = toIndex(hiLong, dim, dimLen, argPos, "upper");
                    derivedArray.slice(dim++, lo, hi);
                } else {
                    long pgIndexLong = rangeFn.getLong(rec);
                    if (pgIndexLong == Numbers.LONG_NULL) {
                        derivedArray.ofNull();
                        return derivedArray;
                    }
                    if (pgIndexLong == 0) {
                        throw CairoException.nonCritical()
                                .position(argPos)
                                .put("array index must be non-zero [dim=").put(dim + 1)
                                .put(", index=").put(pgIndexLong)
                                .put(']');
                    }
                    int pgIndex = (int) pgIndexLong;
                    if (pgIndex != pgIndexLong) {
                        throw CairoException.nonCritical().position(argPos)
                                .put("int overflow on array index [dim=").put(i)
                                .put(", index=").put(pgIndexLong)
                                .put(']');
                    }
                    int index;
                    if (pgIndex < 0) {
                        index = pgIndex + dimLen; // This automatically accounts for 1-based indexing
                    } else {
                        // Decrement the index in the argument because Postgres uses 1-based array indexing
                        index = pgIndex - 1;
                    }
                    derivedArray.subArray(dim, index);
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

        private int toIndex(long indexLong, int dim, int dimLen, int argPos, String boundName) {
            if (indexLong == Long.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            // the "a:b" operator only accepts INT, guaranteeing narrowing cast success
            int index = (int) indexLong;
            assert index == indexLong : "int overflow on interval " + boundName + " bound: " + indexLong;
            if (index < 0) {
                index += dimLen; // This automatically accounts for 1-based indexing
                if (index < 0) {
                    index = 0;
                }
            } else if (index > 0) {
                // Decrement the index in the argument because Postgres uses 1-based array indexing
                index--;
            } else {
                throw CairoException.nonCritical()
                        .position(argPos)
                        .put("array slice bounds must be non-zero [dim=").put(dim + 1)
                        .put(", ")
                        .put(boundName)
                        .put("Bound=").put(index)
                        .put(']');
            }
            return index;
        }
    }
}
