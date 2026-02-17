/*******************************************************************************
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
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class RPadVarcharVarcharFunctionFactory implements FunctionFactory {

    private static final String SIGNATURE = "rpad(ØIØ)";

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
    ) {
        final Function strFunc = args.getQuick(0);
        final Function lenFunc = args.getQuick(1);
        final Function fillTextFunc = args.getQuick(2);
        final int maxLength = configuration.getStrFunctionMaxBufferLength();

        if (strFunc.isConstant() && !fillTextFunc.isConstant()) {
            return new RPadVarcharFuncStrConst(strFunc, lenFunc, fillTextFunc, maxLength);
        }

        if (!strFunc.isConstant() && fillTextFunc.isConstant()) {
            return new RPadVarcharFuncFillTextConst(strFunc, lenFunc, fillTextFunc, maxLength);
        }

        return new RPadVarcharFunc(strFunc, lenFunc, fillTextFunc, maxLength);
    }

    private static Utf8StringSink rPadVarchar0(
            Utf8Sequence str,
            int strLength,
            int len,
            int maxLength,
            Utf8Sequence fillText,
            int fillTextLength,
            Utf8StringSink sink
    ) {
        if (str != null && len >= 0 && fillText != null && fillText.size() > 0) {
            if (len > maxLength) {
                throw CairoException.nonCritical()
                        .put("breached memory limit set for ").put(SIGNATURE)
                        .put(" [maxLength=").put(maxLength)
                        .put(", requiredLength=").put(len).put(']');
            }
            sink.clear();

            if (len > strLength) {
                sink.put(str);
                for (int i = 0, n = (len - strLength) / fillTextLength; i < n; i++) {
                    sink.put(fillText);
                }
                Utf8s.strCpy(fillText, 0, (len - strLength) % fillTextLength, sink);
            } else {
                Utf8s.strCpy(str, strLength - len, strLength, sink);
            }
            return sink;
        }
        return null;
    }

    public static class RPadVarcharFunc extends VarcharFunction implements TernaryFunction {
        protected final Function fillTextFunc;
        protected final Function lenFunc;
        protected final int maxLength;
        protected final Utf8StringSink sinkA = new Utf8StringSink();
        protected final Utf8StringSink sinkB = new Utf8StringSink();
        protected final Function strFunc;

        public RPadVarcharFunc(Function strFunc, Function lenFunc, Function fillTexFunc, int maxLength) {
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
            return "rpad";
        }

        @Override
        public Function getRight() {
            return fillTextFunc;
        }

        @Override
        public Utf8Sequence getVarcharA(final Record rec) {
            return rPadVarchar(strFunc.getVarcharA(rec), lenFunc.getInt(rec), fillTextFunc.getVarcharA(rec), sinkA);
        }

        @Override
        public Utf8Sequence getVarcharB(final Record rec) {
            return rPadVarchar(strFunc.getVarcharB(rec), lenFunc.getInt(rec), fillTextFunc.getVarcharB(rec), sinkB);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Nullable
        private Utf8StringSink rPadVarchar(Utf8Sequence str, int len, Utf8Sequence fillText, Utf8StringSink sink) {
            if (str != null && len >= 0 && fillText != null && fillText.size() > 0) {
                return rPadVarchar0(str, Utf8s.validateUtf8(str), len, maxLength, fillText, Utf8s.validateUtf8(fillText), sink);
            }
            return null;
        }
    }

    public static class RPadVarcharFuncFillTextConst extends RPadVarcharFunc {
        protected final Utf8Sequence fillText;
        protected final int fillTextLength;

        public RPadVarcharFuncFillTextConst(
                @NotNull Function strFunc,
                @NotNull Function lenFunc,
                @NotNull Function fillTexFunc,
                int maxLength
        ) {
            super(strFunc, lenFunc, fillTexFunc, maxLength);
            this.fillText = fillTexFunc.getVarcharA(null);
            this.fillTextLength = fillText != null ? Utf8s.validateUtf8(fillText) : -1;
        }

        @Override
        public Utf8Sequence getVarcharA(final Record rec) {
            return rPadVarchar(strFunc.getVarcharA(rec), lenFunc.getInt(rec), sinkA);
        }

        @Override
        public Utf8Sequence getVarcharB(final Record rec) {
            return rPadVarchar(strFunc.getVarcharB(rec), lenFunc.getInt(rec), sinkB);
        }

        @Nullable
        private Utf8StringSink rPadVarchar(Utf8Sequence str, int len, Utf8StringSink sink) {
            if (str != null && len >= 0 && fillText != null && fillText.size() > 0) {
                return rPadVarchar0(str, Utf8s.validateUtf8(str), len, maxLength, fillText, fillTextLength, sink);
            }
            return null;
        }
    }

    public static class RPadVarcharFuncStrConst extends RPadVarcharFunc {
        protected final Utf8Sequence str;
        protected final int strLength;

        public RPadVarcharFuncStrConst(
                @NotNull Function strFunc,
                @NotNull Function lenFunc,
                @NotNull Function fillTexFunc,
                int maxLength
        ) {
            super(strFunc, lenFunc, fillTexFunc, maxLength);
            this.str = strFunc.getVarcharA(null);
            this.strLength = str != null ? Utf8s.validateUtf8(str) : -1;
        }

        @Override
        public Utf8Sequence getVarcharA(final Record rec) {
            return rPadVarchar(fillTextFunc.getVarcharA(rec), lenFunc.getInt(rec), sinkA);
        }

        @Override
        public Utf8Sequence getVarcharB(final Record rec) {
            return rPadVarchar(strFunc.getVarcharB(rec), lenFunc.getInt(rec), sinkB);
        }

        @Nullable
        private Utf8StringSink rPadVarchar(Utf8Sequence fillText, int len, Utf8StringSink sink) {
            if (str != null && len >= 0 && fillText != null && fillText.size() > 0) {
                return rPadVarchar0(str, strLength, len, maxLength, fillText, Utf8s.validateUtf8(fillText), sink);
            }
            return null;
        }
    }
}