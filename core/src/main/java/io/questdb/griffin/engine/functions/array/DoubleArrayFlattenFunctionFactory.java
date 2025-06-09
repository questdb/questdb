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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class DoubleArrayFlattenFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "flatten(D[]I)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function arrayArg = args.getQuick(0);
        Function dimArg = args.getQuick(1);
        int nDims = ColumnType.decodeArrayDimensionality(arrayArg.getType());
        int dimPos = argPositions.getQuick(1);
        if (dimArg.isConstant()) {
            int flattenDim = dimArg.getInt(null);
            if (flattenDim < 1 || flattenDim > nDims) {
                throw SqlException.$(dimPos, "dimension to flatten out of range [nDims=").put(nDims)
                        .put(", flattenDim=").put(flattenDim).put(']');
            }
        }
        return new Func(arrayArg, dimArg, dimPos);
    }

    private static class Func extends ArrayFunction implements BinaryFunction {
        private final Function arrayArg;
        private final DerivedArrayView borrowedView = new DerivedArrayView();
        private final Function dimArg;
        private final int dimPos;
        private final int nDims;

        public Func(Function arrayArg, Function dimArg, int dimPos) {
            this.arrayArg = arrayArg;
            this.dimArg = dimArg;
            this.dimPos = dimPos;
            this.type = arrayArg.getType();
            this.nDims = ColumnType.decodeArrayDimensionality(type);
        }

        @Override
        public ArrayView getArray(Record rec) {
            ArrayView array = arrayArg.getArray(rec);
            int flattenDim = dimArg.getInt(rec);
            if (flattenDim < 1 || flattenDim > nDims) {
                throw CairoException.nonCritical().position(dimPos)
                        .put("dimension to flatten out of range [nDims=").put(nDims)
                        .put(", flattenDim=").put(flattenDim).put(']');
            }
            borrowedView.of(array);
            borrowedView.flattenDim(flattenDim - 1, dimPos);
            return borrowedView;
        }

        @Override
        public Function getLeft() {
            return arrayArg;
        }

        @Override
        public Function getRight() {
            return dimArg;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("flatten(").val(arrayArg).val(',').val(dimArg).val(')');
        }
    }
}
