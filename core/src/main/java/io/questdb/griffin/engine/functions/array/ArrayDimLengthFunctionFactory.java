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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
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
        final Function arrayArg = args.getQuick(0);
        final Function dimArg = args.getQuick(1);
        final int dimArgPos = argPositions.getQuick(1);
        if (dimArg.isConstant()) {
            final int dim = dimArg.getInt(null);
            dimArg.close();
            if (dim < 1 || dim > ColumnType.ARRAY_NDIMS_LIMIT) {
                throw SqlException.position(dimArgPos).put("array dimension out of bounds [dim=").put(dim).put(']');
            }
            return new ConstFunc(arrayArg, dim, dimArgPos);
        }
        return new Func(arrayArg, dimArg, dimArgPos);
    }

    private static class ConstFunc extends IntFunction implements UnaryFunction {
        private final Function arrayArg;
        private final int dim;
        private final int dimArgPos;

        public ConstFunc(Function arrayArg, int dim, int dimArgPos) {
            this.arrayArg = arrayArg;
            this.dim = dim;
            this.dimArgPos = dimArgPos;
        }

        @Override
        public Function getArg() {
            return arrayArg;
        }

        @Override
        public int getInt(Record rec) {
            ArrayView array = arrayArg.getArray(rec);
            return array.getDimLen(dim - 1);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);

            final int dims = ColumnType.decodeArrayDimensionality(arrayArg.getType());
            if (dim > dims) {
                throw SqlException.position(dimArgPos)
                        .put("array dimension out of bounds [dim=")
                        .put(dim)
                        .put(", dims=")
                        .put(dims)
                        .put(']');
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(FUNCTION_NAME).val('(').val(arrayArg).val(", ").val(dim).val(')');
        }
    }

    private static class Func extends IntFunction implements BinaryFunction {
        private final Function arrayArg;
        private final Function dimArg;
        private final int dimArgPos;
        private int dims;

        public Func(Function arrayArg, Function dimArg, int dimArgPos) {
            this.arrayArg = arrayArg;
            this.dimArg = dimArg;
            this.dimArgPos = dimArgPos;
        }

        @Override
        public int getInt(Record rec) {
            ArrayView array = arrayArg.getArray(rec);
            int dim = dimArg.getInt(rec);
            if (dim < 1 || dim > dims) {
                throw CairoException.nonCritical()
                        .position(dimArgPos)
                        .put("array dimension out of bounds [dim=")
                        .put(dim)
                        .put(", dims=")
                        .put(dims)
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
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);
            this.dims = ColumnType.decodeArrayDimensionality(arrayArg.getType());
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(FUNCTION_NAME).val('(').val(arrayArg).val(", ").val(dimArg).val(')');
        }
    }
}
