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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

public class LeftStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "left(SI)";
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
        final Function countFunc = args.getQuick(1);
        if (countFunc.isConstant()) {
            int count = countFunc.getInt(null);
            if (count != Numbers.INT_NULL) {
                return new ConstCountFunc(strFunc, count);
            } else {
                return StrConstant.NULL;
            }
        }
        return new Func(strFunc, countFunc);
    }

    private static int getPos(int len, int count) {
        return count > -1 ? Math.max(0, Math.min(len, count)) : Math.max(0, len + count);
    }

    private static class ConstCountFunc extends StrFunction implements UnaryFunction {
        private final int count;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();
        private final Function strFunc;

        public ConstCountFunc(Function strFunc, int count) {
            this.strFunc = strFunc;
            this.count = count;
        }

        @Override
        public Function getArg() {
            return strFunc;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return getStr0(rec, sinkA);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStr0(rec, sinkB);
        }

        @Override
        public int getStrLen(Record rec) {
            int len = strFunc.getStrLen(rec);
            return len != TableUtils.NULL_LEN ? getPos(len) : TableUtils.NULL_LEN;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("left(").val(strFunc).val(',').val(count).val(')');
        }

        private int getPos(int len) {
            return LeftStrFunctionFactory.getPos(len, count);
        }

        @Nullable
        private StringSink getStr0(Record rec, StringSink sink) {
            CharSequence str = strFunc.getStrA(rec);
            if (str != null) {
                final int len = str.length();
                final int pos = getPos(len);
                sink.clear();
                sink.put(str, 0, pos);
                return sink;
            }
            return null;
        }
    }

    private static class Func extends StrFunction implements BinaryFunction {
        private final Function countFunc;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();
        private final Function strFunc;

        public Func(Function strFunc, Function countFunc) {
            this.strFunc = strFunc;
            this.countFunc = countFunc;
        }

        @Override
        public Function getLeft() {
            return strFunc;
        }

        @Override
        public String getName() {
            return "left";
        }

        @Override
        public Function getRight() {
            return countFunc;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return getStr0(rec, sinkA);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStr0(rec, sinkB);
        }

        @Override
        public int getStrLen(Record rec) {
            int count = countFunc.getInt(rec);
            int len = strFunc.getStrLen(rec);
            if (len != TableUtils.NULL_LEN && count != Numbers.INT_NULL) {
                return getPos(len, count);
            }
            return TableUtils.NULL_LEN;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Nullable
        private StringSink getStr0(Record rec, StringSink sink) {
            final CharSequence str = strFunc.getStrA(rec);
            final int count = countFunc.getInt(rec);
            if (str != null && count != Numbers.INT_NULL) {
                final int len = str.length();
                final int pos = getPos(len, count);
                sink.clear();
                sink.put(str, 0, pos);
                return sink;
            }
            return null;
        }
    }
}
