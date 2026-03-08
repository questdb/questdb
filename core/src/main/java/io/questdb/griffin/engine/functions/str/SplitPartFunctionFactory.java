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
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;

public class SplitPartFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "split_part(SSI)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final Function strFunc = args.getQuick(0);
        final Function delimiterFunc = args.getQuick(1);
        final Function indexFunc = args.getQuick(2);
        final int indexPosition = argPositions.getQuick(2);

        if (indexFunc.isConstant()) {
            int index = indexFunc.getInt(null);
            if (index == Numbers.INT_NULL) {
                return StrConstant.NULL;
            } else if (index == 0) {
                throw SqlException.$(indexPosition, "field position must not be zero");
            } else {
                return new SplitPartConstIndexFunction(strFunc, delimiterFunc, indexFunc, indexPosition, index);
            }
        } else if (!indexFunc.isRuntimeConstant()) {
            throw SqlException.$(indexPosition, "index must be either a constant expression or a placeholder");
        }
        return new SplitPartFunction(strFunc, delimiterFunc, indexFunc, indexPosition);
    }

    private static void splitToSink(Utf16Sink sink, int index, CharSequence str, CharSequence delimiter) {
        if (index == 0) {
            return;
        }

        int start;
        int end;
        if (index > 0) {
            if (index == 1) {
                start = 0;
            } else {
                start = Chars.indexOf(str, 0, str.length(), delimiter, index - 1);
                if (start == -1) {
                    return;
                }
                start += delimiter.length();
            }

            end = Chars.indexOf(str, start, str.length(), delimiter);
            if (end == -1) {
                end = str.length();
            }
        } else {    // if index is negative, returns index-from-last field
            if (index == -1) {
                end = str.length();
            } else {
                end = Chars.indexOf(str, 0, str.length(), delimiter, index + 1);
                if (end == -1) {
                    return;
                }
            }

            start = Chars.indexOf(str, 0, end, delimiter, -1);
            if (start == -1) {
                start = 0;
            } else {
                start += delimiter.length();
            }
        }

        sink.put(str, start, end);
    }

    private static abstract class AbstractSplitPartFunction extends StrFunction implements TernaryFunction {
        protected final Function delimiterFunc;
        protected final Function indexFunc;
        protected final Function strFunc;
        private final int indexPosition;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public AbstractSplitPartFunction(Function strFunc, Function delimiterFunc, Function indexFunc, int indexPosition) {
            this.strFunc = strFunc;
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
            return strFunc;
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
        public CharSequence getStrA(Record rec) {
            return getStrWithClear(rec, sinkA);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStrWithClear(rec, sinkB);
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

        private StringSink getStrWithClear(Record rec, StringSink sink) {
            sink.clear();
            CharSequence str = strFunc.getStrA(rec);
            CharSequence delimiter = delimiterFunc.getStrA(rec);
            int index = getIndex(rec);
            if (str == null || delimiter == null || index == Numbers.INT_NULL) {
                return null;
            }
            splitToSink(sink, index, str, delimiter);
            return sink;
        }

        abstract int getIndex(Record rec);
    }

    private static class SplitPartConstIndexFunction extends AbstractSplitPartFunction {
        private final int index;

        public SplitPartConstIndexFunction(
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

    private static class SplitPartFunction extends AbstractSplitPartFunction implements TernaryFunction {

        public SplitPartFunction(
                Function strFunc,
                Function delimiterFunc,
                Function indexFunc,
                int indexPosition
        ) {
            super(strFunc, delimiterFunc, indexFunc, indexPosition);
        }

        @Override
        int getIndex(Record rec) {
            return indexFunc.getInt(rec);
        }
    }
}
