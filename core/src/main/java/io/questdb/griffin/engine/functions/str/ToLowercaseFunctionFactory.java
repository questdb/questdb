/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class ToLowercaseFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "to_lowercase(S)";
    }

    @Override
    public Function newInstance(final int position, final ObjList<Function> args, IntList argPositions, final CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new ToLowercaseFunc(args.get(0));
    }

    private static class ToLowercaseFunc extends StrFunction implements UnaryFunction {
        private final Function arg;

        private final StringSink sinkA = new StringSink();

        private final StringSink sinkB = new StringSink();

        public ToLowercaseFunc(final Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public CharSequence getStr(final Record rec) {
            CharSequence str = getArg().getStr(rec);
            if (str == null) {
                return null;
            }

            sinkA.clear();
            Chars.toLowerCase(str, sinkA);
            return sinkA;
        }

        @Override
        public CharSequence getStrB(final Record rec) {
            CharSequence str = getArg().getStr(rec);
            if (str == null) {
                return null;
            }

            sinkB.clear();
            Chars.toLowerCase(str, sinkB);
            return sinkB;
        }

        @Override
        public int getStrLen(final Record rec) {
            return arg.getStrLen(rec);
        }
    }
}
