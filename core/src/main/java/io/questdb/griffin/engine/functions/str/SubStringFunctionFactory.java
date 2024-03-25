/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

public class SubStringFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "substring(SII)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function strFunc = args.getQuick(0);
        final Function startFunc = args.getQuick(1);
        final Function lenFunc = args.getQuick(2);
        if (strFunc.isNullConstant()
                || startFunc.isConstant() && startFunc.getInt(null) == Numbers.INT_NaN
                || lenFunc.isConstant() && lenFunc.getInt(null) == Numbers.INT_NaN) {
            return StrConstant.NULL;
        }
        if (lenFunc.isConstant()) {
            int len = lenFunc.getInt(null);
            if (len < 0) {
                throw SqlException.$(position, "negative substring length is not allowed");
            } else if (len == 0) {
                return StrConstant.EMPTY;
            }
        }
        return new SubStringFunc(strFunc, startFunc, lenFunc);
    }

    private static class SubStringFunc extends StrFunction implements TernaryFunction {
        private final boolean isSimplifiable;
        private final Function lenFunc;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();
        private final Function startFunc;
        private final Function strFunc;

        public SubStringFunc(Function strFunc, Function startFunc, Function lenFunc) {
            this.strFunc = strFunc;
            this.startFunc = startFunc;
            this.lenFunc = lenFunc;

            this.isSimplifiable = startFunc.isConstant() && lenFunc.isConstant()
                    && startFunc.getInt(null) + lenFunc.getInt(null) < 1;
        }

        @Override
        public Function getCenter() {
            return startFunc;
        }

        @Override
        public Function getLeft() {
            return strFunc;
        }

        @Override
        public String getName() {
            return "substring";
        }

        @Override
        public Function getRight() {
            return lenFunc;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return getStr0(rec, sinkA);
        }

        @Override
        public CharSequence getStrB(final Record rec) {
            return getStr0(rec, sinkB);
        }

        @Override
        public int getStrLen(final Record rec) {
            int strLen = strFunc.getStrLen(rec);
            int rawStart = startFunc.getInt(rec);
            int len = lenFunc.getInt(rec);
            if (strLen == TableUtils.NULL_LEN
                    || rawStart == Numbers.INT_NaN
                    || len == Numbers.INT_NaN) {
                return TableUtils.NULL_LEN;
            }

            int start = Math.max(0, rawStart - 1);
            if (len == 0
                    || start > strLen
                    || rawStart + len < 1) {
                return 0;
            }
            int end = Math.min(strLen, rawStart + len - 1);
            return Math.max(0, end - start);
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        @Nullable
        private StringSink getStr0(Record rec, StringSink sink) {
            CharSequence str = strFunc.getStrA(rec);
            if (str == null || isSimplifiable) {
                return null;
            }
            int rawStart = startFunc.getInt(rec);
            int len = lenFunc.getInt(rec);
            if (rawStart == Numbers.INT_NaN || len == Numbers.INT_NaN) {
                return null;
            }
            if (len < 0) {
                throw CairoException.nonCritical().put("negative substring length is not allowed");
            }

            sink.clear();
            int start = Math.max(0, rawStart - 1);
            int end = Math.min(str.length(), rawStart + len - 1);
            if (len == 0 || start >= end) {
                return sink;
            }

            sink.put(str, start, end);
            return sink;
        }
    }
}
