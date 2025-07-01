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
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class DoubleNegArrayFunctionFactory implements FunctionFactory {
    private static final String OPERATOR_NAME = "-";

    @Override
    public String getSignature() {
        return OPERATOR_NAME + "(D[])";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return new Func(args.getQuick(0), configuration);
    }

    private static class Func extends ArrayFunction implements DoubleUnaryArrayAccessor, UnaryFunction {
        private final DirectArray array;
        protected Function arrayArg;
        protected MemoryA memory;

        public Func(Function arrayArg, CairoConfiguration configuration) {
            this.arrayArg = arrayArg;
            this.array = new DirectArray(configuration);
            this.type = arrayArg.getType();
        }

        @Override
        public void applyToElement(ArrayView view, int index) {
            memory.putDouble(-view.getDouble(index));
        }

        @Override
        public void applyToEntireVanillaArray(ArrayView view) {
            FlatArrayView flatView = view.flatView();
            for (int i = view.getFlatViewOffset(), n = view.getFlatViewOffset() + view.getFlatViewLength(); i < n; i++) {
                memory.putDouble(-flatView.getDoubleAtAbsIndex(i));
            }
        }

        @Override
        public void applyToNullArray() {
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            array.close();
        }

        @Override
        public Function getArg() {
            return arrayArg;
        }

        @Override
        public ArrayView getArray(Record rec) {
            ArrayView arr = arrayArg.getArray(rec);
            if (arr.isNull()) {
                array.ofNull();
                return array;
            }

            array.setType(getType());
            array.copyShapeFrom(arr);
            array.applyShape();
            memory = array.startMemoryA();
            calculate(arr);
            return array;
        }

        public String getName() {
            return OPERATOR_NAME;
        }

        @Override
        public boolean isOperator() {
            return true;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }
}
