/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.functions.str;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.engine.functions.BinaryFunction;
import com.questdb.griffin.engine.functions.StrFunction;
import com.questdb.std.Numbers;
import com.questdb.std.ObjList;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;

public class SubStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "substr(SI)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
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
