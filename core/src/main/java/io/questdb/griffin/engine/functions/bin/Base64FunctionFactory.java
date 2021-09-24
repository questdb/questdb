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

package io.questdb.griffin.engine.functions.bin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.BinarySequence;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

public class Base64FunctionFactory implements FunctionFactory {

    static final char[] base64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray();
    static final StringSink buffer = new StringSink();

    static void base64encode(BinarySequence sequence, final int len, CharSink buffer) {
        int pad = 0;
        for (int i = 0; i < len; i += 3) {

            int b = ((sequence.byteAt(i) & 0xFF) << 16) & 0xFFFFFF;
            if (i + 1 < len) {
                b |= (sequence.byteAt(i + 1) & 0xFF) << 8;
            } else {
                pad++;
            }
            if (i + 2 <len) {
                b |= (sequence.byteAt(i + 2) & 0xFF);
            } else {
                pad++;
            }

            for (int j = 0; j < 4 - pad; j++) {
                int c = (b & 0xFC0000) >> 18;
                buffer.put(base64[c]);
                b <<= 6;
            }
        }

        for (int j = 0; j < pad; j++) {
            buffer.put("=");
        }
    }

    @Override
    public String getSignature() {
        return "base64(Ui)";
    }

    @Override
    public Function newInstance(int position,
                                ObjList<Function> args,
                                IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext) throws SqlException {

        final Function maxLengthArg = args.get(1);
        final int maxLength = maxLengthArg.getInt(null);

        if (maxLength < 1) {
            throw SqlException.$(argPositions.getQuick(1), "maxLength should be > 0");
        }

        final Function sequenceArg = args.get(0);
        if (sequenceArg.isConstant()) {
            BinarySequence sequence = sequenceArg.getBin(null);
            final int len = (int)sequenceArg.getBinLen(null);
            final int length = Math.min(len, maxLength);

            buffer.clear();
            base64encode(sequence, length, buffer);
            return StrConstant.newInstance(buffer);
        } else {
            return new Base64Func(sequenceArg, maxLength);
        }
    }

    private static class Base64Func extends StrFunction implements UnaryFunction {
        private final StringSink buffer = new StringSink();
        private final Function data;
        private final int maxLength;

        public Base64Func(final Function data, final int maxLength) {
            this.data = data;
            this.maxLength = maxLength;
        }

        @Override
        public Function getArg() {
            return data;
        }

        @Override
        public CharSequence getStr(final Record rec) {
            final BinarySequence sequence = getArg().getBin(rec);
            final int sequenceLen = (int)getArg().getBinLen(rec);
            buffer.clear();
            base64encode(sequence, Math.min(sequenceLen, this.maxLength), buffer);
            return buffer;
        }

        @Override
        public CharSequence getStrB(final Record rec) {
            return getStr(rec);
        }

        @Override
        public int getStrLen(final Record rec) {
            return buffer.length();
        }
    }
}
