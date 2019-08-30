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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

public class ToCharBinFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "to_char(U)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        return new ToCharBinFunc(position, args.getQuick(0));
    }

    private static class ToCharBinFunc extends StrFunction implements UnaryFunction {
        private final Function arg;
        private final StringSink sink1 = new StringSink();
        private final StringSink sink2 = new StringSink();

        public ToCharBinFunc(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public CharSequence getStr(Record rec) {
            return toSink(rec, sink1);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return toSink(rec, sink2);
        }

        @Override
        public void getStr(Record rec, CharSink sink) {
            Chars.toSink(arg.getBin(rec), sink);
        }

        @Override
        public int getStrLen(Record rec) {
            BinarySequence sequence = arg.getBin(rec);
            if (sequence == null) {
                return -1;
            }

            int len = (int) sequence.length();
            // number of lines
            int incompleteLine = len % 16;
            int count = len / 16 * 57;
            if (incompleteLine > 0) {
                count += incompleteLine * 2 + incompleteLine + 8;
            } else {
                count--; // subtract extra line end we took into account
            }
            return count;
        }

        @Nullable
        private CharSequence toSink(Record rec, StringSink sink) {
            final BinarySequence sequence = arg.getBin(rec);
            if (sequence == null) {
                return null;
            }
            sink.clear();
            Chars.toSink(sequence, sink);
            return sink;
        }

    }
}
