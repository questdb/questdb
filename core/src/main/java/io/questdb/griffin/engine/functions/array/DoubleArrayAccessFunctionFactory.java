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
import io.questdb.cairo.arr.BorrowedArrayView;
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
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function arrayArg = null;
        try {
            arrayArg = args.getQuick(0);
            args.remove(0);
            argPositions.removeIndex(0);
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
                if (argType == ColumnType.INT || argType == ColumnType.SHORT || argType == ColumnType.BYTE) {
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
            Misc.clearObjList(args);
            throw e;
        }
    }

    static class AccessDoubleArrayFunction extends DoubleFunction {

        private final IntList indexArgPositions;
        private final ObjList<Function> indexFns;
        private Function arrayFn;

        AccessDoubleArrayFunction(Function arrayFn, ObjList<Function> indexFns, IntList indexArgPositions) {
            this.arrayFn = arrayFn;
            this.indexFns = new ObjList<>(indexFns);
            this.indexArgPositions = indexArgPositions;
        }

        @Override
        public void close() {
            this.arrayFn = Misc.free(arrayFn);
            for (int n = indexFns.size(), i = 0; i < n; i++) {
                indexFns.getQuick(i).close();
            }
            indexFns.clear();
        }

        @Override
        public double getDouble(Record rec) {
            ArrayView array = arrayFn.getArray(rec);
            int nDims = indexFns.size();
            int flatIndex = 0;
            for (int dim = 0; dim < nDims; dim++) {
                int indexAtDim = indexFns.getQuick(dim).getInt(rec);
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
            sink.val("[](").val(arrayFn);
            for (int n = indexFns.size(), i = 0; i < n; i++) {
                sink.val(',').val(indexFns.getQuick(i));
            }
            sink.val(')');
        }
    }

    static class SliceDoubleArrayFunction extends ArrayFunction {

        private final IntList argPositions;
        private final BorrowedArrayView borrowedView = new BorrowedArrayView();
        private final ObjList<Function> rangeFns;
        private Function arrayFn;

        public SliceDoubleArrayFunction(Function arrayFn, int resultNDims, ObjList<Function> rangeFns, IntList argPositions) {
            this.arrayFn = arrayFn;
            this.rangeFns = new ObjList<>(rangeFns);
            this.argPositions = argPositions;
            this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, resultNDims);
        }

        @Override
        public void close() {
            this.arrayFn = Misc.free(this.arrayFn);
            for (int n = rangeFns.size(), i = 0; i < n; i++) {
                rangeFns.getQuick(i).close();
            }
            rangeFns.clear();
        }

        @Override
        public ArrayView getArray(Record rec) {
            ArrayView array = arrayFn.getArray(rec);
            borrowedView.of(array);
            int dim = 0;
            for (int n = rangeFns.size(), i = 0; i < n; i++) {
                Function rangeFn = rangeFns.getQuick(i);
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
                    borrowedView.slice(dim++, lo, hi, argPos);
                } else {
                    int index = rangeFn.getInt(rec);
                    int dimLen = borrowedView.getDimLen(dim);
                    if (index < 0 || index >= dimLen) {
                        throw CairoException.nonCritical()
                                .position(argPos)
                                .put("array index out of range [dim=").put(i)
                                .put(", index=").put(index)
                                .put(", dimLen=").put(dimLen)
                                .put(']');
                    }
                    borrowedView.slice(dim, index, index + 1, argPos);
                    borrowedView.removeDim(dim);
                }
            }
            return borrowedView;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("[](").val(arrayFn);
            for (int n = rangeFns.size(), i = 0; i < n; i++) {
                sink.val(',').val(rangeFns.getQuick(i));
            }
            sink.val(')');
        }
    }
}
