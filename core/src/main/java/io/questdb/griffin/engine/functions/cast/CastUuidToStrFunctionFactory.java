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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;

public final class CastUuidToStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Zs)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        Function func = args.getQuick(0);
        if (func.isConstant()) {
            StringSink sink = Misc.getThreadLocalSink();
            if (SqlUtil.implicitCastUuidAsStr(func.getLong128Lo(null), func.getLong128Hi(null), sink)) {
                return new StrConstant(Chars.toString(sink));
            } else {
                return StrConstant.NULL;
            }
        }
        return new Func(func);
    }

    public static class Func extends AbstractCastToStrFunction {
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public Func(Function arg) {
            super(arg);
        }

        @Override
        public CharSequence getStrA(Record rec) {
            sinkA.clear();
            return SqlUtil.implicitCastUuidAsStr(arg.getLong128Lo(rec), arg.getLong128Hi(rec), sinkA) ? sinkA : null;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            sinkB.clear();
            return SqlUtil.implicitCastUuidAsStr(arg.getLong128Lo(rec), arg.getLong128Hi(rec), sinkB) ? sinkB : null;
        }

        @Override
        public int getStrLen(Record rec) {
            long lo = arg.getLong128Lo(rec);
            long hi = arg.getLong128Hi(rec);
            return Uuid.isNull(lo, hi) ? TableUtils.NULL_LEN : Uuid.UUID_LENGTH;
        }
    }
}
