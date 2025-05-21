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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.bytes.DirectByteSink;
import org.jetbrains.annotations.Nullable;

public class CastStrToBinFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Su)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        // expression parser will only allow casting 'null' to binary, nothing else.
        return new Func(args.getQuick(0));
    }

    private static class Func extends BinFunction implements UnaryFunction {
        protected final Function arg;
        BinarySequenceByteSink sink;

        public Func(Function arg) {
            this.arg = arg;
            this.sink = new BinarySequenceByteSink(8, true);
        }

        @Override
        public void close() {
            sink.close();
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public BinarySequence getBin(Record rec) {
            CharSequence cs = arg.getStrA(rec);
            sink.clear();
            return parseBinaryLiteral(cs);
        }

        @Override
        public long getBinLen(Record rec) {
            CharSequence cs = arg.getStrA(rec);
            if (cs == null) {
                return -1;
            } else {
                return getBin(rec).length();
            }
        }

        public @Nullable BinarySequence parseBinaryLiteral(@Nullable CharSequence cs) {
            if (cs == null) {
                return null;
            }
            if (!(cs.charAt(0) == '\\' && cs.charAt(1) == 'x')) {
                throw CairoException.nonCritical().put("cannot cast STRING to BINARY [str=").put(cs).put(']');
            }

            // format is
            // \x
            // then pairs of chars per byte
            // can be gaps between pairs

            int i = 2;
            int len = cs.length();
            char c1;
            char c2;

            while (i < len) {
                c1 = cs.charAt(i++);
                if (Character.isWhitespace(c1)) {
                    continue;
                }
                switch (c1) {
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                    case 'a':
                    case 'A':
                    case 'b':
                    case 'B':
                    case 'c':
                    case 'C':
                    case 'd':
                    case 'D':
                    case 'e':
                    case 'E':
                    case 'f':
                    case 'F':
                        c2 = cs.charAt(i++);
                        final byte b = (byte) (((Character.digit(c1, 16) << 4)
                                + Character.digit(c2, 16)));
                        sink.put(b);
                        break;
                    default:
                        throw CairoException.nonCritical().put("cannot cast STRING to BINARY [str=").put(cs).put(']');
                }
            }


            return sink;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val("::binary");
        }

        private static class BinarySequenceByteSink extends DirectByteSink implements BinarySequence {

            public BinarySequenceByteSink(long initialCapacity, boolean alloc) {
                super(initialCapacity, alloc);
            }

            @Override
            public byte byteAt(long index) {
                return super.byteAt((int) index);
            }

            @Override
            public long length() {
                return super.size();
            }

        }
    }
}
