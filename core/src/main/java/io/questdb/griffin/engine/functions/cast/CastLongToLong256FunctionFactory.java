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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;

public class CastLongToLong256FunctionFactory extends FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Lh)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        return new Func(position, args.getQuick(0));
    }

    private static class Func extends Long256Function implements UnaryFunction {
        private final Function arg;
        private final Long256Impl long256a = new Long256Impl();
        private final Long256Impl long256b = new Long256Impl();

        public Func(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public Long256 getLong256A(Record rec) {
            final long value = arg.getLong(rec);
            if (value == Numbers.LONG_NaN) {
                return Long256Impl.NULL_LONG256;
            }
            long256a.setLong0(value);
            return long256a;
        }

        @Override
        public Long256 getLong256B(Record rec) {
            final long value = arg.getLong(rec);
            if (value == Numbers.LONG_NaN) {
                return Long256Impl.NULL_LONG256;
            }
            long256b.setLong0(value);
            return long256b;
        }

        @Override
        public void getLong256(Record rec, CharSink sink) {
            final long value = arg.getLong(rec);
            if (value == Numbers.LONG_NaN) {
                return;
            }
            Numbers.appendLong256(value, 0, 0, 0, sink);
        }
    }
}
