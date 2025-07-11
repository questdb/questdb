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
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class DoubleArrayShiftDefaultNaNFunctionFactory implements FunctionFactory {
    private static final String FUNCTION_NAME = "shift";

    @Override
    public String getSignature() {
        return FUNCTION_NAME + "(D[]I)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return new Func(args.getQuick(0), args.getQuick(1), configuration);
    }

    private static class Func extends ArrayFunction implements BinaryFunction {
        private final DirectArray array;
        private final Function arrayArg;
        private final Function shiftFunction;

        public Func(Function arrayArg, Function shiftFunction, CairoConfiguration configuration) {
            this.arrayArg = arrayArg;
            this.shiftFunction = shiftFunction;
            this.type = arrayArg.getType();
            this.array = new DirectArray(configuration);
        }

        @Override
        public void close() {
            BinaryFunction.super.close();
            Misc.free(array);
        }

        @Override
        public ArrayView getArray(Record rec) {
            ArrayView arr = arrayArg.getArray(rec);
            if (arr.isNull()) {
                array.ofNull();
                return array;
            }

            int offset = shiftFunction.getInt(rec);
            if (offset == 0) {
                return arr;
            }
            array.setType(getType());
            array.copyShapeFrom(arr);
            array.applyShape();
            MemoryA memory = array.startMemoryA();
            if (arr.isVanilla()) {
                DoubleArrayShiftFunctionFactory.applyToEntireVanillaArray(arr, memory, offset, Double.NaN);
            } else {
                DoubleArrayShiftFunctionFactory.applyRecursive(arr, 0, 0, offset, memory, Double.NaN);
            }
            return array;
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
            return shiftFunction;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }
}
