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
import io.questdb.std.Misc;
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
        Function arrayArg = args.getQuick(0);
        args.remove(0);
        argPositions.removeIndex(0);
        int arrayDimCount = ColumnType.decodeArrayDimensionality(arrayArg.getType());
        int accessDimCount = args.size();
        if (accessDimCount > arrayDimCount) {
            throw SqlException
                    .position(argPositions.get(arrayDimCount))
                    .put("too many array coordinates [accessDims=").put(accessDimCount)
                    .put(", arrayDims=").put(arrayDimCount)
                    .put(']');
        }
        return accessDimCount == arrayDimCount
                ? new DoubleArrayAccessFunction(arrayArg, args, argPositions)
                : new DoubleSubArrayFunction(arrayArg, args, argPositions);
    }

    private static class DoubleArrayAccessFunction extends DoubleFunction {

        private final IntList indexArgPositions;
        private final ObjList<Function> indexFns;
        private Function arrayFn;

        DoubleArrayAccessFunction(Function arrayFn, ObjList<Function> indexFns, IntList indexArgPositions) {
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

    private static class DoubleSubArrayFunction extends ArrayFunction {
        private final BorrowedArrayView borrowedView = new BorrowedArrayView();
        private final IntList indexArgPositions;
        private final ObjList<Function> indexFns;
        private Function arrayFn;

        private DoubleSubArrayFunction(Function arrayFn, ObjList<Function> indexFns, IntList indexArgPositions) {
            this.arrayFn = arrayFn;
            this.indexArgPositions = indexArgPositions;
            this.indexFns = new ObjList<>(indexFns);
            int nDimsOriginal = ColumnType.decodeArrayDimensionality(arrayFn.getType());
            this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDimsOriginal - indexFns.size());
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
        public ArrayView getArray(Record rec) {
            ArrayView array = arrayFn.getArray(rec);
            borrowedView.of(array);
            for (int n = indexFns.size(), i = 0; i < n; i++) {
                Function indexFn = indexFns.getQuick(i);
                int arrayIndex = indexFn.getInt(rec);
                borrowedView.asSubArrayAt(arrayIndex, indexArgPositions.get(i));
            }
            return borrowedView;
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
}
