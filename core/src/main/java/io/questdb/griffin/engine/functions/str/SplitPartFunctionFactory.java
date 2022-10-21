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
import io.questdb.cairo.sql.Argument;
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
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

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

        Argument<CharSequence> strParam = Argument.fromDynamicFunction(strFunc, "str", argPositions.getQuick(0), func -> func::getStr);
        Argument<CharSequence> delimiterParam = Argument.fromDynamicFunction(delimiterFunc, "delimiter", argPositions.getQuick(1), func -> func::getStr);
        Argument<Integer> indexParam = Argument.fromRuntimeConstantFunction(indexFunc, "index", argPositions.getQuick(2), func -> func::getInt);

        Function fallback = indexParam.validateConstant((index, paramName, paramPosition) -> {
            if (index == Numbers.INT_NaN) {
                return StrConstant.NULL;
            } else if (index == 0) {
                throw SqlException.$(paramPosition, "field position must not be zero");
            }
            return null;
        });
        if (fallback != null) {
            return fallback;
        }
        indexParam.registerRuntimeConstantValidator((index, paramName, paramPosition) -> {
            if (index == 0) {
                throw SqlException.$(paramPosition, "field position must not be zero");
            }
        });

        return new SplitPartFunction(strParam, delimiterParam, indexParam);
    }

    private static class SplitPartFunction extends StrFunction implements TernaryFunction {
        private final StringSink sink = new StringSink();
        private final StringSink sinkB = new StringSink();

        private final Argument<CharSequence> strParam;
        private final Argument<CharSequence> delimiterParam;
        private final Argument<Integer> indexParam;

        public SplitPartFunction(Argument<CharSequence> strParam,
                                 Argument<CharSequence> delimiterParam,
                                 Argument<Integer> indexParam) {
            this.strParam = strParam;
            this.delimiterParam = delimiterParam;
            this.indexParam = indexParam;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            strParam.init(symbolTableSource, executionContext);
            delimiterParam.init(symbolTableSource, executionContext);
            indexParam.init(symbolTableSource, executionContext);
        }

        @Override
        public Function getLeft() {
            return strParam.getFunc();
        }

        @Override
        public Function getCenter() {
            return delimiterParam.getFunc();
        }

        @Override
        public Function getRight() {
            return indexParam.getFunc();
        }

        @Override
        public CharSequence getStr(Record rec) {
            return getStr0(rec, sink, true);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStr0(rec, sinkB, true);
        }

        @Override
        public void getStr(Record rec, CharSink sink) {
            getStr0(rec, sink, false);
        }

        @Nullable
        private <S extends CharSink> S getStr0(Record rec, S sink, boolean clearSink) {
            CharSequence str = strParam.getValue(rec);
            CharSequence delimiter = delimiterParam.getValue(rec);
            int index = indexParam.getValue(rec);
            if (str == null || delimiter == null || index == Numbers.INT_NaN) {
                return null;
            }
            if (index == 0) {
                return sink;
            }

            int start;
            int end;
            if (index > 0) {
                if (index == 1) {
                    start = 0;
                } else {
                    start = Chars.indexOf(str, 0, str.length(), delimiter, index - 1);
                    if (start == -1) {
                        return sink;
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
                        return sink;
                    }
                }

                start = Chars.indexOf(str, 0, end, delimiter, -1);
                if (start == -1) {
                    start = 0;
                } else {
                    start += delimiter.length();
                }
            }

            if (clearSink && sink instanceof Mutable) {
                ((Mutable) sink).clear();
            }
            sink.put(str, start, end);
            return sink;
        }
    }
}
