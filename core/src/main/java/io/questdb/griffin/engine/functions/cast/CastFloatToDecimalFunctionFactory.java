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
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class CastFloatToDecimalFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Fξ)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new Func(args.getQuick(0), args.getQuick(1).getType(), argPositions.getQuick(0));
    }

    private static class Func extends CastToDecimalFunction {
        private final StringSink sink = new StringSink();

        public Func(Function value, int targetType, int position) {
            super(value, targetType, position);
        }

        protected boolean cast(Record rec) {
            float f = this.arg.getFloat(rec);
            if (!Numbers.isNull(f)) {
                return false;
            }
            sink.clear();
            sink.put(f);
            try {
                decimal.ofString(sink, precision, scale);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(sink, ColumnType.FLOAT, type).position(position);
            }
            return true;
        }
    }
}
