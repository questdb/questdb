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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class DoubleArrayInsertionPointFunctionFactory implements FunctionFactory {
    private static final String FUNCTION_NAME = "insertion_point";

    @Override
    public String getSignature() {
        return FUNCTION_NAME + "(D[]DT)";
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
        final int dims = ColumnType.decodeWeakArrayDimensionality(arrayArg.getType());
        if (dims > 0 && dims != 1) {
            throw SqlException.position(argPositions.getQuick(0)).put("array is not one-dimensional");
        }
        return new Func(arrayArg, argPositions.getQuick(0), args.get(1), args.get(2));
    }

    private static class Func extends IntFunction implements TernaryFunction {
        private final Function aheadEqual;
        private final Function arrayArg;
        private final int arrayArgPos;
        private final Function valueArg;

        public Func(Function arrayArg, int arrayArgPos, Function valueArg, Function afterEqual) {
            this.arrayArg = arrayArg;
            this.arrayArgPos = arrayArgPos;
            this.valueArg = valueArg;
            this.aheadEqual = afterEqual;
        }

        @Override
        public Function getCenter() {
            return valueArg;
        }

        @Override
        public int getInt(Record rec) {
            ArrayView arr = arrayArg.getArray(rec);
            if (arr.isNull()) {
                return Numbers.INT_NULL;
            }
            boolean ahead = aheadEqual.getBool(rec);
            int index = arr.binarySearchDoubleValue1DArray(valueArg.getDouble(rec), ahead);
            return index < 0 ? -index : (ahead ? index + 1 : index + 2);
        }

        @Override
        public Function getLeft() {
            return arrayArg;
        }

        @Override
        public String getName() {
            return FUNCTION_NAME;
        }

        @Override
        public Function getRight() {
            return aheadEqual;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            TernaryFunction.super.init(symbolTableSource, executionContext);
            if (ColumnType.decodeArrayDimensionality(arrayArg.getType()) != 1) {
                throw SqlException.position(arrayArgPos).put("array is not one-dimensional");
            }
        }
    }
}
