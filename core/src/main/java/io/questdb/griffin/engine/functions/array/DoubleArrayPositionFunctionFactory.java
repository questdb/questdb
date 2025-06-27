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
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        Function arrayArg = args.getQuick(0);
        if (ColumnType.decodeArrayDimensionality(arrayArg.getType()) != 1) {
            throw SqlException.position(argPositions.getQuick(0)).put("array is not one-dimensional");
        }

        Function valueArg = args.getQuick(1);
        if (valueArg.isConstant()) {
            double value = valueArg.getDouble(null);
            valueArg.close();
            return Numbers.isFinite(value) ? new ArrayPositionConstFunction(arrayArg, value) : new ArrayPositionConstNaNFunction(arrayArg);
        }
        return new ArrayIndexOfFunction(arrayArg, valueArg);
    }

    static class ArrayIndexOfFunction extends IntFunction implements BinaryFunction {

        protected final Function arrayArg;
        protected final Function valueArg;

        ArrayIndexOfFunction(Function arrayArg, Function valueArg) {
            this.arrayArg = arrayArg;
            this.valueArg = valueArg;
        }

        @Override
        public int getInt(Record rec) {
            ArrayView array = arrayArg.getArray(rec);
            if (array.isNull()) {
                return Numbers.INT_NULL;
            }
            double value = valueArg.getDouble(rec);
            return Numbers.isNull(value) ? array.linearSearchDoubleNull1DArray()
                    : array.linearSearchDoubleValue1DArray(value);
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
        public void toPlan(PlanSink sink) {
            sink.val(FUNCTION_NAME).val('(').val(arrayArg).val(", ").val(valueArg).val(')');
        }
    }

    static class ArrayPositionConstFunction extends IntFunction implements UnaryFunction {

        protected final Function arrayArg;
        protected final double value;

        ArrayPositionConstFunction(Function arrayArg, double value) {
            this.arrayArg = arrayArg;
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
            return arr.linearSearchDoubleValue1DArray(value);
        }

        @Override
        public String getName() {
            return FUNCTION_NAME;
        }
    }

    static class ArrayPositionConstNaNFunction extends IntFunction implements UnaryFunction {

        protected final Function arrayArg;

        ArrayPositionConstNaNFunction(Function arrayArg) {
            this.arrayArg = arrayArg;
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
            return arr.linearSearchDoubleNull1DArray();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(FUNCTION_NAME).val('(').val(arrayArg).val(", Null)");
        }
    }
}
