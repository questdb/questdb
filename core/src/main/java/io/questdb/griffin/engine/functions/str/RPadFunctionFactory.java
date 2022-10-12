/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

public class RPadFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rpad(SI)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new RPadFunc(args.getQuick(0), args.getQuick(1));
    }

    public static class RPadFunc extends StrFunction implements BinaryFunction {

        private final Function strFunc;
        private final Function countFunc;

        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public RPadFunc(Function strFunc, Function countFunc) {
            this.strFunc = strFunc;
            this.countFunc = countFunc;
        }

        @Override
        public Function getLeft() {
            return strFunc;
        }

        @Override
        public Function getRight() {
            return countFunc;
        }

        @Override
        public CharSequence getStr(final Record rec) {
            return rPad(getLeft().getStr(rec),getRight().getInt(rec), sinkA);
        }

        @Override
        public CharSequence getStrB(final Record rec) {
            return rPad(getLeft().getStr(rec),getRight().getInt(rec), sinkB);
        }

        @Override
        public int getStrLen(Record rec) {
            return strFunc.getStrLen(rec);
        }

        @Nullable
        private static CharSequence rPad(CharSequence str, int count, StringSink sink) {
            if (str != null && count != Numbers.INT_NaN) {
                sink.clear();
                sink.put(str);
                sink.put(Chars.repeat(" ",count));
                return sink;
            }
            return null;
        }
    }
}