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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;

public final class VarcharToTimestampFunctionFactory implements FunctionFactory {
    private final static String NAME = "to_timestamp";

    @Override
    public String getSignature() {
        return "to_timestamp(Ø)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function arg = args.getQuick(0);
        return new ToTimestampFunction(arg, ColumnType.TIMESTAMP_MICRO, NAME);
    }

    public static final class ToTimestampFunction extends TimestampFunction implements UnaryFunction {
        private final Function arg;
        private final String name;

        public ToTimestampFunction(Function arg, int timestampType, String name) {
            super(timestampType);
            this.arg = arg;
            this.name = name;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public long getTimestamp(Record rec) {
            final Utf8Sequence value = arg.getVarcharA(rec);
            try {
                return Numbers.parseLong(value);
            } catch (NumericException ignore) {
            }
            return Numbers.LONG_NULL;
        }
    }
}
