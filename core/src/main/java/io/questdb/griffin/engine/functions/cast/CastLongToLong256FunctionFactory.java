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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

public class CastLongToLong256FunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Lh)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new Func(args.getQuick(0));
    }

    private static class Func extends AbstractCastToLong256Function {
        private final Long256Impl long256a = new Long256Impl();
        private final Long256Impl long256b = new Long256Impl();

        public Func(Function arg) {
            super(arg);
        }

        @Override
        public void getLong256(Record rec, CharSink<?> sink) {
            final long value = arg.getLong(rec);
            if (value == Numbers.LONG_NULL) {
                return;
            }
            Numbers.appendLong256(value, 0, 0, 0, sink);
        }

        @Override
        public Long256 getLong256A(Record rec) {
            final long value = arg.getLong(rec);
            if (value == Numbers.LONG_NULL) {
                return Long256Impl.NULL_LONG256;
            }
            long256a.setLow(value);
            return long256a;
        }

        @Override
        public Long256 getLong256B(Record rec) {
            final long value = arg.getLong(rec);
            if (value == Numbers.LONG_NULL) {
                return Long256Impl.NULL_LONG256;
            }
            long256b.setLow(value);
            return long256b;
        }
    }
}
