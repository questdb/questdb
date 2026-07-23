/*+*****************************************************************************
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
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
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
            if (dim == Numbers.INT_NULL) {
                // A NULL dimension is not an out-of-bounds dimension: there is no dimension to
                // measure, so the length is NULL. This mirrors the array access function, where
                // arr[NULL] is NULL rather than an error. Neither arg survives into the constant,
                // so this branch owns both of them. arrayArg may be a constant array literal holding
                // native memory, so failing to close it leaks.
                dimArg.close();
                arrayArg.close();
                return IntConstant.NULL;
            }
            // Every throw below leaves both args to the caller: FunctionParser frees the argument
            // list when newInstance() throws, so closing them here would free them twice.
            if (dim < 1 || dim > ColumnType.ARRAY_NDIMS_LIMIT) {
                throw SqlException.position(dimArgPos).put("array dimension out of bounds [dim=").put(dim).put(']');
            }
            if (arrayArg.isConstant()) {
                // A constant array makes the whole call constant, and FunctionParser folds a constant
                // function by calling getInt(null) on it - it never calls init(), which is where
                // ConstFunc checks the dimension against the array. So a constant array has to be
                // checked here, or the fold reads past the end of the shape header. An array bind
                // variable is not constant, so it does not fold, and init() still checks it once the
                // bound value has given it a type.
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
            dimArg.close();
            return new ConstFunc(arrayArg, dim, dimArgPos);
        }
        return new Func(arrayArg, dimArg, dimArgPos);
    }

    /**
     * Returns the column index when the argument reads a plain array column, so the function can take
     * the dimension length straight out of the shape header: materializing the array loads its shape,
     * resets its strides and builds a flat view, and all of that is thrown away to return one int.
     * Returns -1 for any other argument, which then takes the {@code ArrayView} route.
     */
    private static int arrayColumnIndex(Function arrayArg) {
        // unwrap() rather than a bare instanceof: it peels the memoizer wrappers the code generator
        // inserts around a projection referenced more than once, which a bare check would send down
        // the slow ArrayView route.
        final ColumnFunction cf = ColumnFunction.unwrap(arrayArg);
        return cf != null ? cf.getColumnIndex() : -1;
    }

    private static class ConstFunc extends IntFunction implements UnaryFunction {
        private final Function arrayArg;
        private final int arrayColumnIndex;
        private final int arrayColumnType;
        private final int dim;
        private final int dimArgPos;

        public ConstFunc(Function arrayArg, int dim, int dimArgPos) {
            this.arrayArg = arrayArg;
            this.arrayColumnIndex = arrayColumnIndex(arrayArg);
            this.arrayColumnType = arrayArg.getType();
            this.dim = dim;
            this.dimArgPos = dimArgPos;
        }

        @Override
        public Function getArg() {
            return arrayArg;
        }

        @Override
        public int getInt(Record rec) {
            // dim is already checked against the array's dimensionality: in newInstance() when the
            // array is constant, and in init() otherwise.
            if (arrayColumnIndex >= 0) {
                return rec.getArrayDimLen(arrayColumnIndex, arrayColumnType, dim);
            }
            ArrayView array = arrayArg.getArray(rec);
            if (array.isNull()) {
                return Numbers.INT_NULL;
            }
            return array.getDimLen(dim - 1);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);

            final int dims = ColumnType.decodeWeakArrayDimensionality(arrayArg.getType());
            if (dims > 0 && dim > dims) {
                throw SqlException.position(dimArgPos)
                        .put("array dimension out of bounds [dim=")
                        .put(dim)
                        .put(", dims=")
                        .put(dims)
                        .put(']');
            }
            // getInt()'s arrayColumnIndex >= 0 fast path hands dim to an unchecked shape read, so a
            // plain array column must decode to a concrete dimensionality. Parity with Func.init().
            assert arrayColumnIndex < 0 || dims > 0
                    : "array column with no concrete dimensionality: " + ColumnType.nameOf(arrayArg.getType());
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(FUNCTION_NAME).val('(').val(arrayArg).val(", ").val(dim).val(')');
        }
    }

    private static class Func extends IntFunction implements BinaryFunction {
        private final Function arrayArg;
        private final int arrayColumnIndex;
        private final int arrayColumnType;
        private final Function dimArg;
        private final int dimArgPos;
        private int dims;

        public Func(Function arrayArg, Function dimArg, int dimArgPos) {
            this.arrayArg = arrayArg;
            this.arrayColumnIndex = arrayColumnIndex(arrayArg);
            this.arrayColumnType = arrayArg.getType();
            this.dimArg = dimArg;
            this.dimArgPos = dimArgPos;
        }

        @Override
        public int getInt(Record rec) {
            // Read the dimension before touching the array: a NULL or out-of-bounds dimension needs
            // no array at all.
            int dim = dimArg.getInt(rec);
            if (dim == Numbers.INT_NULL) {
                return Numbers.INT_NULL;
            }
            if (dim < 1 || (dims > 0 && dim > dims)) {
                throw CairoException.nonCritical()
                        .position(dimArgPos)
                        .put("array dimension out of bounds [dim=")
                        .put(dim)
                        .put(", dims=")
                        .put(dims)
                        .put(']');
            }
            if (arrayColumnIndex >= 0) {
                return rec.getArrayDimLen(arrayColumnIndex, arrayColumnType, dim);
            }
            ArrayView array = arrayArg.getArray(rec);
            if (array.isNull()) {
                return Numbers.INT_NULL;
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
            // Read the type live, after super.init(). An arg's type is not final at construction: an
            // array bind variable is typed with weak dims at parse time - a pg array OID carries no
            // dimensionality - and only learns its real shape when the value is bound, which is what
            // init() is for. Decoding a compile-time snapshot here would read -1 dimensions and reject
            // every index. arrayColumnType above stays a snapshot because it is only read on the
            // arrayColumnIndex >= 0 fast path, where the arg is a plain column whose type is final.
            this.dims = ColumnType.decodeWeakArrayDimensionality(arrayArg.getType());
            // getInt()'s fast path hands dim to an unchecked shape read, so it may only run once dim
            // has been bounds-checked against a real dimensionality. decodeWeakArrayDimensionality
            // yields -1 for weak dims and 0 for a NULL type, either of which disables that check - so
            // the fast path is only sound while a plain array column always decodes to dims >= 1.
            assert arrayColumnIndex < 0 || dims > 0
                    : "array column with no concrete dimensionality: " + ColumnType.nameOf(arrayArg.getType());
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(FUNCTION_NAME).val('(').val(arrayArg).val(", ").val(dimArg).val(')');
        }
    }
}
