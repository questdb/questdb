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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;


public class SplitPartVarcharFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "split_part(ØØI)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function varcharFunc = args.getQuick(0);
        final Function delimiterFunc = args.getQuick(1);
        final Function indexFunc = args.getQuick(2);
        final int indexPosition = argPositions.getQuick(2);

        if (indexFunc.isConstant()) {
            int index = indexFunc.getInt(null);
            if (index == Numbers.INT_NULL) {
                return VarcharConstant.NULL;
            } else if (index == 0) {
                throw SqlException.$(indexPosition, "field position must not be zero");
            } else {
                return new SplitPartVarcharConstIndexFunction(varcharFunc, delimiterFunc, indexFunc, indexPosition, index);
            }
        } else if (!indexFunc.isRuntimeConstant()) {
            throw SqlException.$(indexPosition, "index must be either a constant expression or a placeholder");
        }
        return new SplitPartVarcharFunction(varcharFunc, delimiterFunc, indexFunc, indexPosition);
    }

    private static void splitToSink(Utf8Sink sink, int index, Utf8Sequence utf8Str, Utf8Sequence delimiter) {
        if (index == 0) {
            return;
        }

        int size = utf8Str.size();
        int len = Utf8s.length(utf8Str);

        int start;
        int end;
        if (index > 0) {
            if (index == 1) {
                start = 0;
            } else {
                start = Utf8s.indexOf(utf8Str, 0, size, delimiter, index - 1);
                if (start == -1) {
                    return;
                }
                start += delimiter.size();
            }

            end = Utf8s.indexOf(utf8Str, start, size, delimiter);

            if (end == -1) {
                end = len;
            }
        } else { // if index is negative, returns index-from-last field
            if (index == -1) {
                end = size;
            } else {
                end = Utf8s.indexOf(utf8Str, 0, size, delimiter, index + 1);
                if (end == -1) {
                    return;
                }
            }

            start = Utf8s.indexOf(utf8Str, 0, end, delimiter, -1);

            if (start == -1) {
                start = 0;
            } else {
                start += delimiter.size();
            }
        }

        sink.put(utf8Str, start, end);
    }

    private static abstract class AbstractSplitPartVarcharFunction extends VarcharFunction implements TernaryFunction {
        protected final Function delimiterFunc;
        protected final Function indexFunc;
        protected final Function varcharFunc;
        private final int indexPosition;
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();

        public AbstractSplitPartVarcharFunction(Function varcharFunc, Function delimiterFunc, Function indexFunc, int indexPosition) {
            this.varcharFunc = varcharFunc;
            this.delimiterFunc = delimiterFunc;
            this.indexFunc = indexFunc;
            this.indexPosition = indexPosition;
        }

        @Override
        public final Function getCenter() {
            return delimiterFunc;
        }

        @Override
        public final Function getLeft() {
            return varcharFunc;
        }

        @Override
        public String getName() {
            return "split_part";
        }

        @Override
        public final Function getRight() {
            return indexFunc;
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            return getVarcharWithClear(rec, sinkA);
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            return getVarcharWithClear(rec, sinkB);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            TernaryFunction.super.init(symbolTableSource, executionContext);
            if (indexFunc.isRuntimeConstant()) {
                int index = indexFunc.getInt(null);
                if (index == 0) {
                    throw SqlException.$(indexPosition, "field position must not be zero");
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Nullable
        private Utf8StringSink getVarcharWithClear(Record rec, Utf8StringSink sink) {
            sink.clear();

            Utf8Sequence utf8Str = varcharFunc.getVarcharA(rec);
            Utf8Sequence delimiter = delimiterFunc.getVarcharA(rec);
            int index = getIndex(rec);
            if (utf8Str == null || delimiter == null || index == Numbers.INT_NULL) {
                return null;
            }
            splitToSink(sink, index, utf8Str, delimiter);
            return sink;
        }

        abstract int getIndex(Record rec);
    }

    private static class SplitPartVarcharConstIndexFunction extends AbstractSplitPartVarcharFunction {
        private final int index;

        public SplitPartVarcharConstIndexFunction(
                Function strFunc,
                Function delimiterFunc,
                Function indexFunc,
                int indexPosition,
                int index
        ) {
            super(strFunc, delimiterFunc, indexFunc, indexPosition);
            this.index = index;
        }

        @Override
        int getIndex(Record rec) {
            return index;
        }
    }

    private static class SplitPartVarcharFunction extends AbstractSplitPartVarcharFunction implements TernaryFunction {

        public SplitPartVarcharFunction(
                Function varcharFunc,
                Function delimiterFunc,
                Function indexFunc,
                int indexPosition
        ) {
            super(varcharFunc, delimiterFunc, indexFunc, indexPosition);
        }

        @Override
        int getIndex(Record rec) {
            return indexFunc.getInt(rec);
        }
    }
}
