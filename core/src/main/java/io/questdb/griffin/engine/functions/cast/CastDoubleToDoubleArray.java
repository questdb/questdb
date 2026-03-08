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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.SingleElementDoubleArray;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class CastDoubleToDoubleArray implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Dd[])";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function typeFunc = args.getQuick(1);
        final int type = typeFunc.getType();
        if (ColumnType.decodeWeakArrayDimensionality(type) == -1) {
            throw SqlException.$(argPositions.getQuick(1), "cannot cast double to array with unknown number of dimensions");
        }
        return new Func(args.getQuick(0), type);
    }

    public static class Func extends ArrayFunction implements UnaryFunction {
        private final Function arg;
        private final SingleElementDoubleArray array;

        public Func(Function arg, int arrType) {
            super.type = arrType;
            this.arg = arg;
            this.array = new SingleElementDoubleArray(ColumnType.decodeWeakArrayDimensionality(arrType));
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            Misc.free(array);
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public ArrayView getArray(Record rec) {
            double val = arg.getDouble(rec);
            if (Numbers.isNull(val)) {
                return ArrayConstant.NULL;
            }
            array.of(val);
            return array;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getArg()).val("::").val(ColumnType.nameOf(type));
        }
    }
}
