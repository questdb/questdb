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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class DoubleArrayIndexOfAssumeSortedFunctionFactory implements FunctionFactory {
    private static final String FUNCTION_NAME = "indexOfAssumeSorted";

    @Override
    public String getSignature() {
        return FUNCTION_NAME + "(D[]D)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        Function arrayArg = args.getQuick(0);
        Function valueArg = args.getQuick(1);
        int arrayArgPos = argPositions.getQuick(1);
        if (valueArg.isConstant()) {
            int nDims = ColumnType.decodeArrayDimensionality(arrayArg.getType());
            if (nDims != 1) {
                throw SqlException.position(arrayArgPos).put("array is not one-dimensional");
            }
            double value = valueArg.getDouble(null);
            valueArg.close();
            return Numbers.isFinite(value) ? new ArrayIndexOfAssumeSortedConstFunction(arrayArg, value) : new ArrayIndexOfAssumeSortedConstNaNFunction(arrayArg);
        }
        return new ArrayIndexOfAssumeSortedFunction(arrayArg, valueArg);
    }

    static class ArrayIndexOfAssumeSortedConstFunction extends DoubleArrayIndexOfFunctionFactory.ArrayIndexOfConstFunction implements UnaryFunction {

        ArrayIndexOfAssumeSortedConstFunction(Function arrayArg, double value) {
            super(arrayArg, value);
        }

        @Override
        public int getInt(Record rec) {
            return arrayArg.getArray(rec).binarySearchDoubleValue1DArray(value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(FUNCTION_NAME).val('(').val(arrayArg).val(", ").val(value).val(')');
        }
    }

    static class ArrayIndexOfAssumeSortedConstNaNFunction extends DoubleArrayIndexOfFunctionFactory.ArrayIndexOfConstNaNFunction {

        ArrayIndexOfAssumeSortedConstNaNFunction(Function arrayArg) {
            super(arrayArg);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(FUNCTION_NAME).val('(').val(arrayArg).val(", Null)");
        }
    }

    static class ArrayIndexOfAssumeSortedFunction extends DoubleArrayIndexOfFunctionFactory.ArrayIndexOfFunction {

        ArrayIndexOfAssumeSortedFunction(Function arrayArg, Function valueArg) {
            super(arrayArg, valueArg);
        }

        @Override
        public int getInt(Record rec) {
            ArrayView array = arrayArg.getArray(rec);
            double value = valueArg.getDouble(rec);
            return Numbers.isNull(value) ? array.linearSearchDoubleNull1DArray()
                    : array.binarySearchDoubleValue1DArray(value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(FUNCTION_NAME).val('(').val(arrayArg).val(", ").val(valueArg).val(')');
        }
    }
}
