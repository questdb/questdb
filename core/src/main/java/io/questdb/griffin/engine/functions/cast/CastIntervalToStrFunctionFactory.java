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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class CastIntervalToStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Δs)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function intervalFunc = args.getQuick(0);
        if (intervalFunc.isConstant()) {
            StringSink sink = Misc.getThreadLocalSink();
            final Interval interval = intervalFunc.getInterval(null);
            if (Interval.NULL.equals(interval)) {
                return StrConstant.NULL;
            }
            interval.toSink(sink, intervalFunc.getType());
            return new StrConstant(Chars.toString(sink));
        }
        return new Func(args.getQuick(0));
    }

    public static class Func extends AbstractCastToStrFunction {
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public Func(Function arg) {
            super(arg);
        }

        @Override
        public CharSequence getStrA(Record rec) {
            final Interval value = arg.getInterval(rec);
            if (!Interval.NULL.equals(value)) {
                sinkA.clear();
                value.toSink(sinkA, arg.getType());
                return sinkA;
            }
            return null;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            final Interval value = arg.getInterval(rec);
            if (!Interval.NULL.equals(value)) {
                sinkB.clear();
                value.toSink(sinkB, arg.getType());
                return sinkB;
            }
            return null;
        }
    }
}
