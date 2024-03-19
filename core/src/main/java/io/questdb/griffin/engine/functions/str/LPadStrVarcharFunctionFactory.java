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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.*;
import org.jetbrains.annotations.Nullable;

public class LPadStrVarcharFunctionFactory implements FunctionFactory {

    private static final String SIGNATURE = "lpad(ØIØ)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function strFunc = args.getQuick(0);
        final Function lenFunc = args.getQuick(1);
        final Function fillTextFunc = args.getQuick(2);
        final int maxLength = configuration.getStrFunctionMaxBufferLength();
        return new LPadVarcharFunc(strFunc, lenFunc, fillTextFunc, maxLength);
    }

    public static class LPadVarcharFunc extends VarcharFunction implements TernaryFunction {

        private final Function fillTextFunc;
        private final Function lenFunc;
        private final int maxLength;
        private final Utf8StringSink sink = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();
        private final Function strFunc;

        public LPadVarcharFunc(Function strFunc, Function lenFunc, Function fillTexFunc, int maxLength) {
            this.strFunc = strFunc;
            this.lenFunc = lenFunc;
            this.fillTextFunc = fillTexFunc;
            this.maxLength = maxLength;
        }

        @Override
        public Function getCenter() {
            return lenFunc;
        }

        @Override
        public Function getLeft() {
            return strFunc;
        }

        @Override
        public String getName() {
            return "lpad";
        }

        @Override
        public Function getRight() {
            return fillTextFunc;
        }

        @Override
        public Utf8Sequence getVarcharA(final Record rec) {
            return lPadVarchar(strFunc.getVarcharA(rec), lenFunc.getInt(rec), fillTextFunc.getVarcharA(rec), sink);
        }

        @Override
        public void getVarchar(Record rec, Utf8Sink utf8Sink) {
            utf8Sink.put(lPadVarchar(strFunc.getVarcharA(rec), lenFunc.getInt(rec), fillTextFunc.getVarcharA(rec), sinkB));
        }

        @Override
        public Utf8Sequence getVarcharB(final Record rec) {
            return lPadVarchar(strFunc.getVarcharA(rec), lenFunc.getInt(rec), fillTextFunc.getVarcharA(rec), sinkB);
        }

        @Nullable
        private Utf8StringSink lPadVarchar(Utf8Sequence str, int len, Utf8Sequence fillText, Utf8StringSink sink) {
            if (str != null && len >= 0 && fillText != null && fillText.size() > 0) {
                if (len > maxLength) {
                    throw CairoException.nonCritical()
                            .put("breached memory limit set for ").put(SIGNATURE)
                            .put(" [maxLength=").put(maxLength)
                            .put(", requiredLength=").put(len).put(']');
                }
                sink.clear();

                final int length = Utf8s.validateUtf8(str);
                if (len > length) {
                    final int fillTextLen = Utf8s.validateUtf8(fillText);
                    for (int i = 0, n = (len - length) / fillTextLen; i < n; i++) {
                        sink.put(fillText);
                    }
                    Utf8s.strCpy(fillText, 0, (len - length) % fillTextLen, sink);
                    sink.put(str);
                } else {
                    Utf8s.strCpy(str, 0, len, sink);
                }
                return sink;
            }
            return null;
        }
    }
}