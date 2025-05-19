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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cutlass.pgwire.modern.DoubleArrayParser;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class CastStrToDoubleArrayFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Sd[])";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function typeFun = args.getQuick(1);
        int type = typeFun.getType();
        return new Func(args.getQuick(0), type);
    }

    public static class Func extends ArrayFunction implements UnaryFunction {
        private final int dims;
        private final Function function;
        private final DoubleArrayParser parser = new DoubleArrayParser();

        public Func(Function fun, int type) {
            super.type = type;
            this.function = fun;
            this.dims = ColumnType.decodeArrayDimensionality(type);
        }

        @Override
        public Function getArg() {
            return function;
        }

        @Override
        public ArrayView getArray(Record rec) {
            CharSequence str = function.getStrA(rec);
            assert str != null; // for now
            assert str.length() > 0; // for now
            try {
                parser.of(str, dims);
            } catch (IllegalArgumentException e) {
                parser.of(null);
            }
            return parser;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getArg()).val("::double");
            for (int i = 0; i < dims; i++) {
                sink.val("[]");
            }
        }
    }
}
