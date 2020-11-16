/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

public class SubStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "substr(SI)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new Func(position, args.getQuick(0), args.getQuick(1));
    }

    private static class Func extends StrFunction implements BinaryFunction {

        private final StringSink sink = new StringSink();
        private final StringSink sinkB = new StringSink();
        private final Function var;
        private final Function start;

        public Func(int position, Function var, Function start) {
            super(position);
            this.var = var;
            this.start = start;
        }

        @Override
        public Function getLeft() {
            return var;
        }

        @Override
        public Function getRight() {
            return start;
        }

        @Override
        public CharSequence getStr(Record rec) {
            CharSequence str = var.getStr(rec);
            if (str == null) {
                return null;
            }

            int start = this.start.getInt(rec);
            if (start == Numbers.INT_NaN) {
                return null;
            }

            sink.clear();
            int len = str.length();
            if (start > -1 && start < len) {
                sink.put(str, start, len);
            }
            return sink;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            CharSequence str = var.getStrB(rec);
            if (str == null) {
                return null;
            }

            int start = this.start.getInt(rec);
            if (start == Numbers.INT_NaN) {
                return null;
            }

            sinkB.clear();
            int len = str.length();
            if (start > -1 && start < len) {
                sinkB.put(str, start, len);
            }
            return sinkB;
        }

        @Override
        public void getStr(Record rec, CharSink sink) {
            CharSequence str = var.getStr(rec);
            if (str == null) {
                return;
            }

            int start = this.start.getInt(rec);
            if (start == Numbers.INT_NaN) {
                return;
            }

            int len = str.length();
            if (start > -1 && start < len) {
                sink.put(str, start, len);
            }
        }

        @Override
        public int getStrLen(Record rec) {
            int len = var.getStrLen(rec);
            if (len == -1) {
                return len;
            }

            int start = this.start.getInt(rec);
            if (start == Numbers.INT_NaN) {
                return -1;
            }

            return start > -1 && start < len ? len - start : 0;
        }
    }
}
