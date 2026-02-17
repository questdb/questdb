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

package io.questdb.griffin.engine.functions.uuid;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.UuidFunction;
import io.questdb.griffin.engine.functions.constants.UuidConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public final class LongsToUuidFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "to_uuid(LL)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function loLong = args.getQuick(0);
        final Function hiLong = args.getQuick(1);
        if (loLong.isConstant() && hiLong.isConstant()) {
            return new UuidConstant(loLong.getLong(null), hiLong.getLong(null));
        }
        return new LongsToUuidFunction(loLong, hiLong);
    }

    private static class LongsToUuidFunction extends UuidFunction implements BinaryFunction {

        private final Function hi;
        private final Function lo;

        public LongsToUuidFunction(Function loLong, Function hiLong) {
            this.lo = loLong;
            this.hi = hiLong;
        }

        @Override
        public Function getLeft() {
            return lo;
        }

        @Override
        public long getLong128Hi(Record rec) {
            return hi.getLong(rec);
        }

        @Override
        public long getLong128Lo(Record rec) {
            return lo.getLong(rec);
        }

        @Override
        public String getName() {
            return "to_uuid";
        }

        @Override
        public Function getRight() {
            return hi;
        }
    }
}
