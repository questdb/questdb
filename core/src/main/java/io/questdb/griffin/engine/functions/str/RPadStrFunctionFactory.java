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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

public class RPadStrFunctionFactory implements FunctionFactory {

    private static final String SIGNATURE = "rpad(SIS)";

    @Override
    public String getSignature() {
        return SIGNATURE;
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
        final Function lenFunc = args.getQuick(1);
        final Function fillTextFunc = args.getQuick(2);
        final int maxLength = configuration.getStrFunctionMaxBufferLength();
        return new RPadStrFunc(strFunc, lenFunc, fillTextFunc, maxLength);
    }

    public static class RPadStrFunc extends StrFunction implements TernaryFunction {

        private final Function strFunc;
        private final Function lenFunc;
        private final Function fillTextFunc;
        private final int maxLength;
        private final StringSink sink = new StringSink();
        private final StringSink sinkB = new StringSink();

        public RPadStrFunc(Function strFunc, Function lenFunc, Function fillTexFunc, int maxLength) {
            this.strFunc = strFunc;
            this.lenFunc = lenFunc;
            this.fillTextFunc = fillTexFunc;
            this.maxLength = maxLength;
        }

        @Override
        public Function getLeft() {
            return strFunc;
        }

        @Override
        public Function getCenter() {
            return lenFunc;
        }

        @Override
        public Function getRight() {
            return fillTextFunc;
        }

        @Override
        public CharSequence getStr(final Record rec) {
            return rPadStr(strFunc.getStr(rec), lenFunc.getInt(rec), fillTextFunc.getStr(rec), sink);
        }

        @Override
        public CharSequence getStrB(final Record rec) {
            return rPadStr(strFunc.getStr(rec), lenFunc.getInt(rec), fillTextFunc.getStr(rec), sinkB);
        }

        @Override
        public int getStrLen(Record rec) {
            final CharSequence str = strFunc.getStr(rec);
            final int len = lenFunc.getInt(rec);
            final CharSequence fillText = fillTextFunc.getStr(rec);
            if (str != null && len >= 0 && fillText != null && fillText.length() > 0) {
                return len;
            } else {
                return TableUtils.NULL_LEN;
            }
        }

        @Nullable
        private StringSink rPadStr(CharSequence str, int len, CharSequence fillText, StringSink sink) {
            if (str != null && len >= 0 && fillText != null && fillText.length() > 0) {
                if (len > maxLength) {
                    throw CairoException.nonCritical()
                            .put("breached memory limit set for ").put(SIGNATURE)
                            .put(" [maxLength=").put(maxLength)
                            .put(", requiredLength=").put(len).put(']');
                }
                sink.clear();
                if (len > str.length()) {
                    sink.put(str);
                    final int fillTextLen = fillText.length();
                    for (int i = 0, n = (len - str.length()) / fillTextLen; i < n; i++) {
                        sink.put(fillText);
                    }
                    for (int i = 0, n = (len - str.length()) % fillTextLen; i < n; i++) {
                        sink.put(fillText.charAt(i));
                    }
                } else {
                    sink.put(str, str.length() - len, str.length());
                }
                return sink;
            }
            return null;
        }
    }
}