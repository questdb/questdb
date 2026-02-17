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
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class DoubleArrayPositionFunctionFactory implements FunctionFactory {
    private static final String FUNCTION_NAME = "array_position";

    @Override
    public String getSignature() {
        return FUNCTION_NAME + "(D[]D)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function arrayArg = args.getQuick(0);
        final int arrayArgPos = argPositions.getQuick(0);
        final Function valueArg = args.getQuick(1);
        final int dims = ColumnType.decodeWeakArrayDimensionality(arrayArg.getType());
        if (dims > 0 && dims != 1) {
            throw SqlException.position(argPositions.getQuick(0)).put("array is not one-dimensional");
        }

        if (valueArg.isConstant()) {
            final double value = valueArg.getDouble(null);
            valueArg.close();
            return Numbers.isNull(value)
                    ? new ArrayPositionConstNullFunc(arrayArg, arrayArgPos)
                    : new ArrayPositionConstFunc(arrayArg, arrayArgPos, value);
        }
        return new ArrayIndexOfFunc(arrayArg, arrayArgPos, valueArg);
    }

    private static int linearSearchNaN(ArrayView array) {
        if (array.isNull() || array.isEmpty()) {
            return Numbers.INT_NULL;
        }
        for (int i = 0, dimLen = array.getDimLen(0); i < dimLen; i++) {
            if (Numbers.isNull(array.getDouble(i))) {
                return i;
            }
        }
        return Numbers.INT_NULL;
    }

    private static int linearSearchValue(ArrayView array, double value) {
        if (array.isNull() || array.isEmpty()) {
            return Numbers.INT_NULL;
        }
        if (array.isVanilla()) {
            return array.flatView().linearSearch(value, array.getFlatViewOffset(), array.getFlatViewLength());
        } else {
            int stride = array.getStride(0);
            int index = 0;
            for (int i = 0, n = array.getDimLen(0); i < n; i++) {
                double d = array.getDouble(index);
                if (Numbers.isFinite(d) && Math.abs(d - value) <= Numbers.DOUBLE_TOLERANCE) {
                    return i;
                }
                index += stride;
            }
        }

        return Numbers.INT_NULL;
    }

    private static class ArrayIndexOfFunc extends IntFunction implements BinaryFunction {
        private final Function arrayArg;
        private final int arrayArgPos;
        private final Function valueArg;

        ArrayIndexOfFunc(Function arrayArg, int arrayArgPos, Function valueArg) {
            this.arrayArg = arrayArg;
            this.arrayArgPos = arrayArgPos;
            this.valueArg = valueArg;
        }

        @Override
        public int getInt(Record rec) {
            ArrayView array = arrayArg.getArray(rec);
            if (array.isNull()) {
                return Numbers.INT_NULL;
            }
            double value = valueArg.getDouble(rec);
            int index = Numbers.isNull(value) ? linearSearchNaN(array) : linearSearchValue(array, value);
            return Numbers.INT_NULL == index ? Numbers.INT_NULL : index + 1;
        }

        @Override
        public Function getLeft() {
            return arrayArg;
        }

        @Override
        public Function getRight() {
            return valueArg;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);
            if (ColumnType.decodeArrayDimensionality(arrayArg.getType()) != 1) {
                throw SqlException.position(arrayArgPos).put("array is not one-dimensional");
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(FUNCTION_NAME).val('(').val(arrayArg).val(", ").val(valueArg).val(')');
        }
    }

    private static class ArrayPositionConstFunc extends IntFunction implements UnaryFunction {
        private final Function arrayArg;
        private final int arrayArgPos;
        private final double value;

        ArrayPositionConstFunc(Function arrayArg, int arrayArgPos, double value) {
            this.arrayArg = arrayArg;
            this.arrayArgPos = arrayArgPos;
            this.value = value;
        }

        @Override
        public Function getArg() {
            return arrayArg;
        }

        @Override
        public int getInt(Record rec) {
            ArrayView arr = arrayArg.getArray(rec);
            if (arr.isNull()) {
                return Numbers.INT_NULL;
            }
            int pos = linearSearchValue(arr, value);
            return Numbers.INT_NULL == pos ? Numbers.INT_NULL : pos + 1;
        }

        @Override
        public String getName() {
            return FUNCTION_NAME;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            if (ColumnType.decodeArrayDimensionality(arrayArg.getType()) != 1) {
                throw SqlException.position(arrayArgPos).put("array is not one-dimensional");
            }
        }
    }

    private static class ArrayPositionConstNullFunc extends IntFunction implements UnaryFunction {
        private final Function arrayArg;
        private final int arrayArgPos;

        ArrayPositionConstNullFunc(Function arrayArg, int arrayArgPos) {
            this.arrayArg = arrayArg;
            this.arrayArgPos = arrayArgPos;
        }

        @Override
        public Function getArg() {
            return arrayArg;
        }

        @Override
        public int getInt(Record rec) {
            ArrayView arr = arrayArg.getArray(rec);
            if (arr.isNull()) {
                return Numbers.INT_NULL;
            }
            return linearSearchNaN(arr) + 1;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            if (ColumnType.decodeArrayDimensionality(arrayArg.getType()) != 1) {
                throw SqlException.position(arrayArgPos).put("array is not one-dimensional");
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(FUNCTION_NAME).val('(').val(arrayArg).val(", null)");
        }
    }
}
