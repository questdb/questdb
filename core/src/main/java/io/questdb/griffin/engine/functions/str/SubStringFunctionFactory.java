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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

public class SubStringFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "substring(SII)";
    }

    @Override
    public Function newInstance(final int position, final ObjList<Function> args, IntList argPositions, final CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function strFunc = args.getQuick(0);
        final Function startFunc = args.getQuick(1);
        final Function lenFunc = args.getQuick(2);
        if (lenFunc.isConstant()) {
            int len = lenFunc.getInt(null);
            if (len < 0) {  // Numbers.INT_NaN = Integer.MIN_VALUE is also involved
                return StrConstant.NULL;
            }
            // on len = 0, still return null if str is null
        }
        return new SubStringFunc(strFunc, startFunc, lenFunc);
    }

    private static class SubStringFunc extends StrFunction implements TernaryFunction {

        private final Function strFunc;
        private final Function startFunc;
        private final Function lenFunc;

        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public SubStringFunc(Function strFunc, Function startFunc, Function lenFunc) {
            this.strFunc = strFunc;
            this.startFunc = startFunc;
            this.lenFunc = lenFunc;
        }

        @Override
        public Function getLeft() {
            return strFunc;
        }

        @Override
        public Function getCenter() {
            return startFunc;
        }

        @Override
        public Function getRight() {
            return lenFunc;
        }

        @Override
        public CharSequence getStr(Record rec) {
            return getStr0(rec, sinkA);
        }

        @Override
        public CharSequence getStrB(final Record rec) {
            return getStr0(rec, sinkB);
        }

        @Override
        public int getStrLen(final Record rec) {
            int strLen = strFunc.getStrLen(rec);
            int start = Math.max(1, startFunc.getInt(rec));
            int len = lenFunc.getInt(rec);
            if (strLen == TableUtils.NULL_LEN || len < 0) {
                return TableUtils.NULL_LEN;
            }
            if (len == 0 || start > strLen) {
                return 0;
            }
            return Math.min(strLen - start + 1, len);
        }

        @Nullable
        private StringSink getStr0(Record rec, StringSink sink) {
            CharSequence str = strFunc.getStr(rec);
            if (str == null) {
                return null;
            }
            int len = lenFunc.getInt(rec);
            if (len < 0) {  // Numbers.INT_NaN = Integer.MIN_VALUE is also involved
                return null;
            }
            sink.clear();
            int start = Math.max(1, startFunc.getInt(rec));
            int end = Math.min(str.length(), start - 1 + len);
            if (len == 0 || start > end) {
                return sink;
            }
            sink.put(str, start - 1, end);
            return sink;
        }
    }
}
