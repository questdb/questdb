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
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.Nullable;

public class CastTimestampToVarcharFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Nø)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function func = args.getQuick(0);
        if (func.isConstant()) {
            StringSink sink = Misc.getThreadLocalSink();
            sink.put(func.getTimestamp(null));
            return new VarcharConstant(Chars.toString(sink));
        }
        return new Func(args.getQuick(0), ColumnType.getTimestampType(args.getQuick(0).getType()));
    }

    public static class Func extends AbstractCastToVarcharFunction {
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();
        private final TimestampDriver timestampDriver;

        public Func(Function arg, int timestampType) {
            super(arg);
            timestampDriver = ColumnType.getTimestampDriver(timestampType);
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            return toSink(rec, sinkA);
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            return toSink(rec, sinkB);
        }

        private @Nullable Utf8StringSink toSink(Record rec, Utf8StringSink sink) {
            final long value = arg.getTimestamp(rec);
            if (value == Numbers.LONG_NULL) {
                return null;
            }
            sink.clear();
            timestampDriver.append(sink, value);
            return sink;
        }
    }
}
