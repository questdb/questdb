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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class ArrayDimLengthFunctionFactory implements FunctionFactory {
    private static final String FUNCTION_NAME = "dim_length";

    @Override
    public String getSignature() {
        return FUNCTION_NAME + "(D[]I)";
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
        int dimArgPos = argPositions.getQuick(1);
        if (dimArg.isConstant()) {
            int dim = dimArg.getInt(null);
            dimArg.close();
            int nDims = ColumnType.decodeArrayDimensionality(arrayArg.getType());
            if (dim < 1 || dim > nDims) {
                throw SqlException.position(dimArgPos)
                        .put("array dimension out of bounds [dim=")
                        .put(dim)
                        .put(", nDims=")
                        .put(nDims)
                        .put(']');
            }
            return new ArrayDimLengthConstFunction(arrayArg, dim);
        }
        return new ArrayDimLengthFunction(arrayArg, dimArg, dimArgPos);
    }

    static class ArrayDimLengthConstFunction extends IntFunction implements UnaryFunction {
        private final Function arrayArg;
        private final int dimConstArg;

        ArrayDimLengthConstFunction(Function arrayArg, int dimConstArg) {
            this.arrayArg = arrayArg;
            this.dimConstArg = dimConstArg;
        }

        @Override
        public Function getArg() {
            return arrayArg;
        }

        @Override
        public int getInt(Record rec) {
            ArrayView array = arrayArg.getArray(rec);
            return array.getDimLen(dimConstArg - 1);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(FUNCTION_NAME).val('(').val(arrayArg).val(", ").val(dimConstArg).val(')');
        }
    }

    static class ArrayDimLengthFunction extends IntFunction implements BinaryFunction {
        private final Function arrayArg;
        private final Function dimArg;
        private final int dimArgPos;
        private final int nDims;

        ArrayDimLengthFunction(Function arrayArg, Function dimArg, int dimArgPos) {
            this.arrayArg = arrayArg;
            this.dimArg = dimArg;
            this.nDims = ColumnType.decodeArrayDimensionality(arrayArg.getType());
            this.dimArgPos = dimArgPos;
        }

        @Override
        public int getInt(Record rec) {
            ArrayView array = arrayArg.getArray(rec);
            int dim = dimArg.getInt(rec);
            if (dim < 1 || dim > nDims) {
                throw CairoException.nonCritical()
                        .position(dimArgPos)
                        .put("array dimension out of bounds [dim=")
                        .put(dim)
                        .put(", nDims=")
                        .put(nDims)
                        .put(']');
            }
            return array.getDimLen(dim - 1);
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
        public void toPlan(PlanSink sink) {
            sink.val(FUNCTION_NAME).val('(').val(arrayArg).val(", ").val(dimArg).val(')');
        }
    }
}
