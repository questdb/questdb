/*+*****************************************************************************
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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

public class CastIntToVarcharFunctionFactory implements FunctionFactory {

    /**
     * Prints {@code operand} in decimal, INT_NULL included: the body widens to long because {@link
     * Numbers#append(CharSink, int)} prints INT_NULL as null. The function does not call this
     * method: it returns NULL for INT_NULL and prints other values in the int form, which gives the
     * same digits.
     */
    public static void value(CharSink<?> sink, int operand) {
        Numbers.append(sink, operand, false);
    }

    @Override
    public String getSignature() {
        return "cast(Iø)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function intFunc = args.getQuick(0);
        if (intFunc.isConstant()) {
            StringSink sink = Misc.getThreadLocalSink();
            sink.put(intFunc.getInt(null));
            return new VarcharConstant(Chars.toString(sink));
        }
        return new Func(args.getQuick(0));
    }

    public static class Func extends AbstractCastToVarcharFunction {
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();

        public Func(Function arg) {
            super(arg);
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            final int value = arg.getInt(rec);
            if (value != Numbers.INT_NULL) {
                sinkA.clear();
                sinkA.put(value);
                return sinkA;
            }
            return null;
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            final int value = arg.getInt(rec);
            if (value != Numbers.INT_NULL) {
                sinkB.clear();
                sinkB.put(value);
                return sinkB;
            }
            return null;
        }
    }
}
